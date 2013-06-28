"""
This module is the main part of the library. It houses the Scheduler class and related exceptions.
"""

from threading import Thread, Event, Lock
from datetime import datetime, timedelta
from logging import getLogger
from collections import Mapping, Iterable
import os
import sys

from pkg_resources import iter_entry_points

from apscheduler.util import *
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.job import Job, MaxInstancesReachedError
from apscheduler.events import *
from apscheduler.threadpool import ThreadPool

logger = getLogger(__name__)


class SchedulerAlreadyRunningError(Exception):
    """
    Raised when attempting to start or configure the scheduler when it's already running.
    """

    def __str__(self):
        return 'Scheduler is already running'


class Scheduler(object):
    """
    This class is responsible for scheduling jobs and triggering their execution.
    """

    _stopped = False
    _thread = None
    _plugins = dict((ep.name, ep) for ep in iter_entry_points('apscheduler.triggers'))

    def __init__(self, gconfig={}, **options):
        self._wakeup = Event()
        self._jobstores = {}
        self._jobstores_lock = Lock()
        self._listeners = []
        self._listeners_lock = Lock()
        self._pending_jobs = []
        self._triggers = {}
        self.configure(gconfig, **options)

    def configure(self, gconfig={}, **options):
        """
        Reconfigures the scheduler with the given options. Can only be done when the scheduler isn't running.
        """
        if self.running:
            raise SchedulerAlreadyRunningError

        # Set general options
        config = combine_opts(gconfig, 'apscheduler.', options)
        self.misfire_grace_time = int(config.pop('misfire_grace_time', 1))
        self.coalesce = asbool(config.pop('coalesce', True))
        self.daemonic = asbool(config.pop('daemonic', True))
        self.standalone = asbool(config.pop('standalone', False))

        # Configure the thread pool
        if 'threadpool' in config:
            self._threadpool = maybe_ref(config['threadpool'])
        else:
            threadpool_opts = combine_opts(config, 'threadpool.')
            self._threadpool = ThreadPool(**threadpool_opts)

        # Configure job stores
        jobstore_opts = combine_opts(config, 'jobstore.')
        jobstores = {}
        for key, value in jobstore_opts.items():
            store_name, option = key.split('.', 1)
            opts_dict = jobstores.setdefault(store_name, {})
            opts_dict[option] = value

        for alias, opts in jobstores.items():
            classname = opts.pop('class')
            cls = maybe_ref(classname)
            jobstore = cls(**opts)
            self.add_jobstore(jobstore, alias, True)

    def start(self):
        """
        Starts the scheduler in a new thread.

        In threaded mode (the default), this method will return immediately after starting the scheduler thread.

        In standalone mode, this method will block until there are no more scheduled jobs.
        """
        if self.running:
            raise SchedulerAlreadyRunningError

        # Create a RAMJobStore as the default if there is no default job store
        if not 'default' in self._jobstores:
            self.add_jobstore(MemoryJobStore(), 'default', True)

        # Schedule all pending jobs
        for job, jobstore in self._pending_jobs:
            self._real_add_job(job, jobstore, False)
        del self._pending_jobs[:]

        self._stopped = False
        if self.standalone:
            self._main_loop()
        else:
            self._thread = Thread(target=self._main_loop, name='APScheduler')
            self._thread.setDaemon(self.daemonic)
            self._thread.start()

    def shutdown(self, wait=True, shutdown_threadpool=True,
                 close_jobstores=True):
        """
        Shuts down the scheduler and terminates the thread. Does not interrupt any currently running jobs.

        :param wait: ``True`` to wait until all currently executing jobs have finished (if ``shutdown_threadpool`` is
                     also ``True``)
        :param shutdown_threadpool: ``True`` to shut down the thread pool
        :param close_jobstores: ``True`` to close all job stores after shutdown
        """
        if not self.running:
            return

        self._stopped = True
        self._wakeup.set()

        # Shut down the thread pool
        if shutdown_threadpool:
            self._threadpool.shutdown(wait)

        # Wait until the scheduler thread terminates
        if self._thread:
            self._thread.join()

        # Close all job stores
        if close_jobstores:
            for jobstore in itervalues(self._jobstores):
                jobstore.close()

    @property
    def running(self):
        return not self._stopped and self._thread and self._thread.isAlive()

    def add_jobstore(self, jobstore, alias, quiet=False):
        """
        Adds a job store to this scheduler.

        :param jobstore: job store to be added
        :param alias: alias for the job store
        :param quiet: True to suppress scheduler thread wakeup
        :type jobstore: instance of :class:`~apscheduler.jobstores.base.JobStore`
        :type alias: str
        """
        with self._jobstores_lock:
            if alias in self._jobstores:
                raise KeyError('Alias "%s" is already in use' % alias)
            self._jobstores[alias] = jobstore
            jobstore.load_jobs()

        # Notify listeners that a new job store has been added
        self._notify_listeners(JobStoreEvent(EVENT_JOBSTORE_ADDED, alias))

        # Notify the scheduler so it can scan the new job store for jobs
        if not quiet:
            self._wakeup.set()

    def remove_jobstore(self, alias, close=True):
        """
        Removes the job store by the given alias from this scheduler.

        :param close: ``True`` to close the job store after removing it
        :type alias: str
        """
        with self._jobstores_lock:
            jobstore = self._jobstores.pop(alias)
            if not jobstore:
                raise KeyError('No such job store: %s' % alias)

        # Close the job store if requested
        if close:
            jobstore.close()

        # Notify listeners that a job store has been removed
        self._notify_listeners(JobStoreEvent(EVENT_JOBSTORE_REMOVED, alias))

    def add_listener(self, callback, mask=EVENT_ALL):
        """
        Adds a listener for scheduler events. When a matching event occurs, ``callback`` is executed with the event
        object as its sole argument. If the ``mask`` parameter is not provided, the callback will receive events of all
        types.

        :param callback: any callable that takes one argument
        :param mask: bitmask that indicates which events should be listened to
        """
        with self._listeners_lock:
            self._listeners.append((callback, mask))

    def remove_listener(self, callback):
        """
        Removes a previously added event listener.
        """
        with self._listeners_lock:
            for i, (cb, _) in enumerate(self._listeners):
                if callback == cb:
                    del self._listeners[i]

    def _notify_listeners(self, event):
        with self._listeners_lock:
            listeners = tuple(self._listeners)

        for cb, mask in listeners:
            if event.code & mask:
                try:
                    cb(event)
                except:
                    logger.exception('Error notifying listener')

    def _real_add_job(self, job, jobstore, wakeup):
        # Recalculate the next run time
        job.compute_next_run_time(datetime.now())

        # Add the job to the given job store
        store = self._jobstores.get(jobstore)
        if not store:
            raise KeyError('No such job store: %s' % jobstore)
        store.add_job(job)

        # Notify listeners that a new job has been added
        event = JobStoreEvent(EVENT_JOBSTORE_JOB_ADDED, jobstore, job)
        self._notify_listeners(event)

        logger.info('Added job "%s" to job store "%s"', job, jobstore)

        # Notify the scheduler about the new job
        if wakeup:
            self._wakeup.set()

    def add_job(self, func, trigger, trigger_args=(), args=None, kwargs=None, misfire_grace_time=None, coalesce=None,
                name=None, max_runs=None, max_instances=1, jobstore='default'):
        """
        Adds the given job to the job list and notifies the scheduler thread.

        The ``func`` argument can be given either as a callable object or a textual reference in the
        ``package.module:some.object`` format, where the first half (separated by ``:``) is an importable module and the
        second half is a reference to the callable object, relative to the module.

        The ``trigger`` argument can either be:

        # the plugin name of the trigger (e.g. "cron"), in which case you should provide ``trigger_args`` as well
        # an instance of the trigger

        :param trigger: trigger that determines when ``func`` is called
        :param trigger_args: arguments given to the constructor of the trigger class
        :param func: callable (or a textual reference to one) to run at the given time
        :param args: list of positional arguments to call func with
        :param kwargs: dict of keyword arguments to call func with
        :param jobstore: alias of the job store to store the job in
        :param name: name of the job
        :param misfire_grace_time: seconds after the designated run time that the job is still allowed to be run
        :param coalesce: run once instead of many times if the scheduler determines that the job should be run more than once
                         in succession
        :param max_runs: maximum number of times this job is allowed to be triggered
        :param max_instances: maximum number of concurrently running instances allowed for this job
        :rtype: :class:`~apscheduler.job.Job`
        """
        # Argument sanity checking
        if args is not None and (not isinstance(args, Iterable) and not isinstance(args, str)):
            raise TypeError('args must be an iterable')
        if kwargs is not None and not isinstance(kwargs, Mapping):
            raise TypeError('kwargs must be a dict-like object')
        if misfire_grace_time is not None and misfire_grace_time <= 0:
            raise ValueError('misfire_grace_time must be a positive value')
        if max_runs is not None and max_runs <= 0:
            raise ValueError('max_runs must be a positive value')
        if max_instances <= 0:
            raise ValueError('max_instances must be a positive value')

        # If trigger is a string, resolve it to a class, possibly by loading an entry point if necessary
        if isinstance(trigger, str):
            try:
                trigger_cls = self._triggers[trigger]
            except KeyError:
                if trigger in self._plugins:
                    trigger_cls = self._triggers[trigger] = self._plugins[trigger].load()
                    if not callable(getattr(trigger_cls, 'get_next_fire_time')):
                        raise TypeError('The trigger entry point does not point to a trigger class')
                else:
                    raise KeyError('No trigger by the name "%s" was found' % trigger)

            if isinstance(trigger_args, Mapping):
                trigger = trigger_cls(**trigger_args)
            elif isinstance(trigger_args, Iterable):
                trigger = trigger_cls(*trigger_args)
            else:
                raise ValueError('trigger_args must either be a dict-like object or an iterable')
        elif not callable(getattr(trigger, 'get_next_fire_time')):
            raise TypeError('Expected a trigger instance, got %s instead' % trigger.__class__.__name__)

        # Replace with scheduler level defaults if values are missing
        if misfire_grace_time is None:
            misfire_grace_time = self.misfire_grace_time
        if coalesce is None:
            coalesce = self.coalesce

        args = tuple(args) if args is not None else ()
        kwargs = dict(kwargs) if kwargs is not None else {}
        job = Job(trigger, func, args, kwargs, misfire_grace_time, coalesce, name, max_runs, max_instances)

        # Ensure that dead-on-arrival jobs are never added
        if job.compute_next_run_time(datetime.now()) is None:
            raise ValueError('Not adding job since it would never be run')

        # Don't really add jobs to job stores before the scheduler is up and running
        if not self.running:
            self._pending_jobs.append((job, jobstore))
            logger.info('Adding job tentatively -- it will be properly scheduled when the scheduler starts')
        else:
            self._real_add_job(job, jobstore, True)

        return job

    def scheduled_job(self, trigger, trigger_args=(), args=None, kwargs=None, jobstore='default',
                      misfire_grace_time=None, coalesce=None, name=None, max_runs=None, max_instances=1):
        """A decorator version of :meth:`add_job`."""
        def inner(func):
            func.job = self.add_job(func, trigger, trigger_args, args, kwargs, misfire_grace_time, coalesce,
                                    name, max_runs, max_instances, jobstore)
            return func
        return inner

    def _remove_job(self, job, alias, jobstore):
        jobstore.remove_job(job)

        # Notify listeners that a job has been removed
        event = JobStoreEvent(EVENT_JOBSTORE_JOB_REMOVED, alias, job)
        self._notify_listeners(event)

        logger.info('Removed job "%s"', job)

    def get_jobs(self):
        """
        Returns a list of all scheduled jobs.

        :return: list of :class:`~apscheduler.job.Job` objects
        """
        with self._jobstores_lock:
            jobs = []
            for jobstore in itervalues(self._jobstores):
                jobs.extend(jobstore.jobs)
            return jobs

    def unschedule_job(self, job):
        """
        Removes a job, preventing it from being run any more.
        """
        with self._jobstores_lock:
            for alias, jobstore in iteritems(self._jobstores):
                if job in list(jobstore.jobs):
                    self._remove_job(job, alias, jobstore)
                    return

        raise KeyError('Job "%s" is not scheduled in any job store' % job)

    def unschedule_func(self, func):
        """
        Removes all jobs that would execute the given function.
        """
        found = False
        with self._jobstores_lock:
            for alias, jobstore in iteritems(self._jobstores):
                for job in list(jobstore.jobs):
                    if job.func == func:
                        self._remove_job(job, alias, jobstore)
                        found = True

        if not found:
            raise KeyError('The given function is not scheduled in this scheduler')

    def print_jobs(self, out=None):
        """
        Prints out a textual listing of all jobs currently scheduled on this
        scheduler.

        :param out: a file-like object to print to (defaults to **sys.stdout** if nothing is given)
        """
        out = out or sys.stdout
        job_strs = []
        with self._jobstores_lock:
            for alias, jobstore in iteritems(self._jobstores):
                job_strs.append('Jobstore %s:' % alias)
                if jobstore.jobs:
                    for job in jobstore.jobs:
                        job_strs.append('    %s' % job)
                else:
                    job_strs.append('    No scheduled jobs')

        out.write(os.linesep.join(job_strs) + os.linesep)

    def _run_job(self, job, run_times):
        """
        Acts as a harness that runs the actual job code in a thread.
        """
        for run_time in run_times:
            # See if the job missed its run time window, and handle possible
            # misfires accordingly
            difference = datetime.now() - run_time
            grace_time = timedelta(seconds=job.misfire_grace_time)
            if difference > grace_time:
                # Notify listeners about a missed run
                event = JobEvent(EVENT_JOB_MISSED, job, run_time)
                self._notify_listeners(event)
                logger.warning('Run time of job "%s" was missed by %s', job, difference)
            else:
                try:
                    job.add_instance()
                except MaxInstancesReachedError:
                    event = JobEvent(EVENT_JOB_MISSED, job, run_time)
                    self._notify_listeners(event)
                    logger.warning('Execution of job "%s" skipped: maximum number of running instances reached (%d)',
                                   job, job.max_instances)
                    break

                logger.info('Running job "%s" (scheduled at %s)', job, run_time)

                try:
                    retval = job.func(*job.args, **job.kwargs)
                except:
                    # Notify listeners about the exception
                    exc, tb = sys.exc_info()[1:]
                    event = JobEvent(EVENT_JOB_ERROR, job, run_time, exception=exc, traceback=tb)
                    self._notify_listeners(event)

                    logger.exception('Job "%s" raised an exception', job)
                else:
                    # Notify listeners about successful execution
                    event = JobEvent(EVENT_JOB_EXECUTED, job, run_time, retval=retval)
                    self._notify_listeners(event)

                    logger.info('Job "%s" executed successfully', job)

                job.remove_instance()

                # If coalescing is enabled, don't attempt any further runs
                if job.coalesce:
                    break

    def _process_jobs(self, now):
        """
        Iterates through jobs in every jobstore, starts pending jobs and figures out the next wakeup time.
        """
        next_wakeup_time = None
        with self._jobstores_lock:
            for alias, jobstore in iteritems(self._jobstores):
                for job in tuple(jobstore.jobs):
                    run_times = job.get_run_times(now)
                    if run_times:
                        self._threadpool.submit(self._run_job, job, run_times)

                        # Increase the job's run count
                        if job.coalesce:
                            job.runs += 1
                        else:
                            job.runs += len(run_times)

                        # Update the job, but don't keep finished jobs around
                        if job.compute_next_run_time(now + timedelta(microseconds=1)):
                            jobstore.update_job(job)
                        else:
                            self._remove_job(job, alias, jobstore)

                    if not next_wakeup_time:
                        next_wakeup_time = job.next_run_time
                    elif job.next_run_time:
                        next_wakeup_time = min(next_wakeup_time, job.next_run_time)
            return next_wakeup_time

    def _main_loop(self):
        """Executes jobs on schedule."""

        logger.info('Scheduler started')
        self._notify_listeners(SchedulerEvent(EVENT_SCHEDULER_START))

        self._wakeup.clear()
        while not self._stopped:
            logger.debug('Looking for jobs to run')
            now = datetime.now()
            next_wakeup_time = self._process_jobs(now)

            # Sleep until the next job is scheduled to be run,
            # a new job is added or the scheduler is stopped
            if next_wakeup_time is not None:
                wait_seconds = time_difference(next_wakeup_time, now)
                logger.debug('Next wakeup is due at %s (in %f seconds)', next_wakeup_time, wait_seconds)
                self._wakeup.wait(wait_seconds)
                self._wakeup.clear()
            elif self.standalone:
                logger.debug('No jobs left; shutting down scheduler')
                self.shutdown()
                break
            else:
                logger.debug('No jobs; waiting until a job is added')
                self._wakeup.wait()
                self._wakeup.clear()

        logger.info('Scheduler has been shut down')
        self._notify_listeners(SchedulerEvent(EVENT_SCHEDULER_SHUTDOWN))
