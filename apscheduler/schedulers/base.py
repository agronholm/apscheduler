from __future__ import print_function
from abc import ABCMeta, abstractmethod
from threading import Lock
from datetime import datetime, timedelta
from logging import getLogger
from collections import Mapping, Iterable
from inspect import ismethod, isfunction
import os
import sys

from pkg_resources import iter_entry_points
from dateutil.tz import tzlocal
from six import u, itervalues, iteritems
import six

from apscheduler.schedulers import SchedulerAlreadyRunningError, SchedulerNotRunningError
from apscheduler.util import *
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.job import Job, MaxInstancesReachedError
from apscheduler.events import *
from apscheduler.threadpool import ThreadPool

try:
    from inspect import getfullargspec as getargspec
except ImportError:
    from inspect import getargspec


class BaseScheduler(six.with_metaclass(ABCMeta)):
    """Base class for all schedulers."""

    _stopped = True
    _plugins = dict((ep.name, ep) for ep in iter_entry_points('apscheduler.triggers'))

    #
    # Public API
    #

    def __init__(self, gconfig={}, **options):
        super(BaseScheduler, self).__init__()
        self._jobstores = {}
        self._jobstores_lock = self._create_lock()
        self._listeners = []
        self._listeners_lock = self._create_lock()
        self._pending_jobs = []
        self._triggers = {}
        self.configure(gconfig, **options)

    def configure(self, gconfig={}, **options):
        """
        Reconfigures the scheduler with the given options. Can only be done when the scheduler isn't running.
        """
        if self.running:
            raise SchedulerAlreadyRunningError

        config = combine_opts(gconfig, 'apscheduler.', options)
        self._configure(config)

    @abstractmethod
    def start(self):
        """Starts the scheduler. The details of this process depend on the implementation."""

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
        self.logger.info('Scheduler started')

        # Notify listeners that the scheduler has been started
        self._notify_listeners(SchedulerEvent(EVENT_SCHEDULER_START))

    @abstractmethod
    def shutdown(self, wait=True):
        """
        Shuts down the scheduler. Does not interrupt any currently running jobs.

        :param wait: ``True`` to wait until all currently executing jobs have finished
        """
        if not self.running:
            raise SchedulerNotRunningError

        self._stopped = True

        # Shut down the thread pool
        self._threadpool.shutdown(wait)

        # Close all job stores
        for jobstore in itervalues(self._jobstores):
            jobstore.close()

        self.logger.info('Scheduler has been shut down')
        self._notify_listeners(SchedulerEvent(EVENT_SCHEDULER_SHUTDOWN))

    @property
    def running(self):
        return not self._stopped

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

        # Notify listeners that a new job store has been added
        self._notify_listeners(JobStoreEvent(EVENT_JOBSTORE_ADDED, alias))

        # Notify the scheduler so it can scan the new job store for jobs
        if not quiet and self.running:
            self._wakeup()

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

    def add_job(self, func, trigger, trigger_args=(), args=None, kwargs=None, id=None, name=None,
                misfire_grace_time=None, coalesce=None, max_runs=None, max_instances=1, jobstore='default'):
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
        :param id: explicit identifier for the job (for modifying it later)
        :param name: textual description of the job
        :param jobstore: alias of the job store to store the job in
        :param misfire_grace_time: seconds after the designated run time that the job is still allowed to be run
        :param coalesce: run once instead of many times if the scheduler determines that the job should be run more than
                         once in succession
        :param max_runs: maximum number of times this job is allowed to be triggered
        :param max_instances: maximum number of concurrently running instances allowed for this job
        :type id: str/unicode
        :type args: list/tuple
        :type jobstore: str/unicode
        :type misfire_grace_time: int
        :type kwargs: dict
        :type coalesce: bool
        :type max_runs: int
        :type max_instances: int
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
                trigger = trigger_cls(self.trigger_defaults, **trigger_args)
            elif isinstance(trigger_args, Iterable):
                trigger = trigger_cls(self.trigger_defaults, *trigger_args)
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
        job = Job(trigger, func, args, kwargs, id, misfire_grace_time, coalesce, name, max_runs, max_instances)
        job.attach_scheduler(self, jobstore)

        # Make sure the callable can handle the given arguments
        self._check_callable_args(job.func, args, kwargs)

        # Ensure that dead-on-arrival jobs are never added
        if job.compute_next_run_time(self._current_time()) is None:
            raise ValueError('Not adding job since it would never be run')

        # Don't really add jobs to job stores before the scheduler is up and running
        if not self.running:
            self._pending_jobs.append((job, jobstore))
            self.logger.info('Adding job tentatively -- it will be properly scheduled when the scheduler starts')
        else:
            self._real_add_job(job, jobstore, True)

        return job

    def scheduled_job(self, trigger, trigger_args=(), args=None, kwargs=None, id=None, name=None,
                      misfire_grace_time=None, coalesce=None, max_runs=None, max_instances=1, jobstore='default'):
        """A decorator version of :meth:`add_job`."""

        def inner(func):
            self.add_job(func, trigger, trigger_args, args, kwargs, id, misfire_grace_time, coalesce, name, max_runs,
                         max_instances, jobstore)
            return func
        return inner

    def modify_job(self, job_id, jobstore='default', **changes):
        """
        Modifies the properties of a single job. Modifications are passed to this method as extra keyword arguments.

        :param job_id: the identifier of the job
        :param jobstore: alias of the job store
        """

        # Sanity check for the changes
        for attr, value in six.iteritems(changes):
            if attr.startswith('_'):
                raise ValueError('Cannot modify protected attributes')
            if not attr in Job.__slots__:
                raise ValueError('Job has no attribute named "%s"' % attr)

        with self._jobstores_lock:
            # Check if the job is among the pending jobs
            for job, store in self._pending_jobs:
                if job.id == job_id:
                    for attr, value in six.iteritems(changes):
                        setattr(job, attr, value)
                    return

            store = self._jobstores[jobstore]
            store.modify_job(job_id, changes)

    def get_jobs(self, jobstore=None, pending=None):
        """
        Returns a list of pending jobs (if the scheduler hasn't been started yet) and scheduled jobs,
        either from a specific job store or from all of them.

        :param jobstore: alias of the job store
        :param pending: ``False`` to leave out pendin jobs (jobs that are waiting for the scheduler start to be added to
                        their respective job stores), ``True`` to only include pending jobs, anything else to return
                        both
        :return: list of :class:`~apscheduler.job.Job` objects
        """

        with self._jobstores_lock:
            jobs = []

            if pending is not False:
                for job, alias in self._pending_jobs:
                    if jobstore is None or alias == jobstore:
                        jobs.append(job)

            if pending is not True:
                jobstores = [jobstore] if jobstore else self._jobstores
                for alias, store in six.iteritems(jobstores):
                    for job in store.get_all_jobs():
                        job.attach_scheduler(self, alias)
                        jobs.append(job)

            return jobs

    def remove_job(self, job_id, jobstore='default'):
        """
        Removes a job, preventing it from being run any more.

        :param job_id: the identifier of the job
        :param jobstore: alias of the job store
        """

        with self._jobstores_lock:
            # Check if the job is among the pending jobs
            for i, (job, store) in enumerate(self._pending_jobs):
                if job.id == job_id:
                    del self._pending_jobs[i]
                    return

            self._jobstores[jobstore].remove_job(job_id)

    def remove_all_jobs(self, jobstore=None):
        """
        Removes all jobs from the specified job store, or all job stores if none is given.

        :param jobstore: alias of the job store
        """

        with self._jobstores_lock:
            jobstores = [jobstore] if jobstore else self._jobstores
            for alias in jobstores:
                self._jobstores[alias].remove_all_jobs()

    def print_jobs(self, jobstore=None, out=None):
        """
        Prints out a textual listing of all jobs currently scheduled on either all job stores or just a specific one.

        :param jobstore: alias of the job store
        :param out: a file-like object to print to (defaults to **sys.stdout** if nothing is given)
        """
        out = out or sys.stdout
        with self._jobstores_lock:
            if self._pending_jobs:
                print(six.u('Pending jobs:'), file=out)
                for job, alias in self._pending_jobs:
                    if jobstore is None or alias == jobstore:
                        print(six.u('    %s') % job, file=out)

            for alias, store in iteritems(self._jobstores):
                if jobstore is None or alias == jobstore:
                    print(six.u('Jobstore %s:') % alias, file=out)
                    jobs = store.get_all_jobs()
                    if jobs:
                        for job in jobs:
                            print(six.u('    %s') % job, file=out)
                    else:
                        print(six.u('    No scheduled jobs'), file=out)

    #
    # Protected API
    #

    def _configure(self, config):
        # Set general options
        self.logger = maybe_ref(config.pop('logger', None)) or getLogger('apscheduler')
        self.misfire_grace_time = int(config.pop('misfire_grace_time', 1))
        self.coalesce = asbool(config.pop('coalesce', True))
        self.timezone = astimezone(config.pop('timezone', None)) or tzlocal()

        # Set trigger defaults
        self.trigger_defaults = {'timezone': self.timezone}

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

    def _notify_listeners(self, event):
        with self._listeners_lock:
            listeners = tuple(self._listeners)

        for cb, mask in listeners:
            if event.code & mask:
                try:
                    cb(event)
                except:
                    self.logger.exception('Error notifying listener')

    def _real_add_job(self, job, jobstore, wakeup):
        # Recalculate the next run time
        job.compute_next_run_time(self._current_time())

        # Add the job to the given job store
        store = self._jobstores.get(jobstore)
        if not store:
            raise KeyError('No such job store: %s' % jobstore)
        store.add_job(job)

        # Notify listeners that a new job has been added
        event = JobStoreEvent(EVENT_JOBSTORE_JOB_ADDED, jobstore, job)
        self._notify_listeners(event)

        self.logger.info('Added job "%s" to job store "%s"', job, jobstore)

        # Notify the scheduler about the new job
        if wakeup:
            self._wakeup()

    def _remove_job(self, job, alias, jobstore):
        jobstore.remove_job(job.id)

        # Notify listeners that a job has been removed
        event = JobStoreEvent(EVENT_JOBSTORE_JOB_REMOVED, alias, job)
        self._notify_listeners(event)

        self.logger.info('Removed job "%s"', job)

    @staticmethod
    def _check_callable_args(func, args, kwargs):
        """Ensures that the given callable can be called with the given arguments."""

        if not isfunction(func) and not ismethod(func) and hasattr(func, '__call__'):
            func = func.__call__
        argspec = getargspec(func)
        argspec_args = argspec.args[1:] if ismethod(func) else argspec.args
        varkw = getattr(argspec, 'varkw', None) or getattr(argspec, 'keywords', None)
        kwargs_set = frozenset(kwargs)
        mandatory_args = frozenset(argspec_args[:-len(argspec.defaults)] if argspec.defaults else argspec_args)
        mandatory_args_matches = frozenset(argspec_args[:len(args)])
        mandatory_kwargs_matches = set(kwargs).intersection(mandatory_args)
        kwonly_args = frozenset(getattr(argspec, 'kwonlyargs', []))
        kwonly_defaults = frozenset(getattr(argspec, 'kwonlydefaults', None) or ())

        # Make sure there are no conflicts between args and kwargs
        pos_kwargs_conflicts = mandatory_args_matches.intersection(mandatory_kwargs_matches)
        if pos_kwargs_conflicts:
            raise ValueError('The following arguments are supplied in both args and kwargs: %s' %
                             ', '.join(pos_kwargs_conflicts))

        # Check that the number of positional arguments minus the number of matched kwargs matches the argspec
        missing_args = mandatory_args - mandatory_args_matches.union(mandatory_kwargs_matches)
        if missing_args:
            raise ValueError('The following arguments are not supplied: %s' % ', '.join(missing_args))

        # Check that the callable can accept the given number of positional arguments
        if not argspec.varargs and len(args) > len(argspec_args):
            raise ValueError('The list of positional arguments is longer than the target callable can handle '
                             '(allowed: %d, given in args: %d)' % (len(argspec_args), len(args)))

        # Check that the callable can accept the given keyword arguments
        if not varkw:
            unmatched_kwargs = kwargs_set - frozenset(argspec_args).union(kwonly_args)
            if unmatched_kwargs:
                raise ValueError('The target callable does not accept the following keyword arguments: %s' %
                                 ', '.join(unmatched_kwargs))

        # Check that all keyword-only arguments have been supplied
        unmatched_kwargs = kwonly_args - kwargs_set - kwonly_defaults
        if unmatched_kwargs:
            raise ValueError('The following keyword-only arguments have not been supplied in kwargs: %s' %
                             ', '.join(unmatched_kwargs))

    @abstractmethod
    def _wakeup(self):
        """Triggers :meth:`_process_jobs` to be run in an implementation specific manner."""

    def _create_lock(self):
        return Lock()

    def _current_time(self):
        return datetime.now(self.timezone)

    def _run_job(self, job, run_times):
        """Acts as a harness that runs the actual job code in the thread pool."""

        for run_time in run_times:
            # See if the job missed its run time window, and handle possible
            # misfires accordingly
            difference = self._current_time() - run_time
            grace_time = timedelta(seconds=job.misfire_grace_time)
            if difference > grace_time:
                # Notify listeners about a missed run
                event = JobEvent(EVENT_JOB_MISSED, job, run_time)
                self._notify_listeners(event)
                self.logger.warning('Run time of job "%s" was missed by %s', job, difference)
            else:
                try:
                    job.add_instance()
                except MaxInstancesReachedError:
                    event = JobEvent(EVENT_JOB_MISSED, job, run_time)
                    self._notify_listeners(event)
                    self.logger.warning(
                        'Execution of job "%s" skipped: maximum number of running instances reached (%d)', job,
                        job.max_instances)
                    break

                self.logger.info('Running job "%s" (scheduled at %s)', job, run_time)

                try:
                    retval = job.func(*job.args, **job.kwargs)
                except:
                    # Notify listeners about the exception
                    exc, tb = sys.exc_info()[1:]
                    event = JobEvent(EVENT_JOB_ERROR, job, run_time, exception=exc, traceback=tb)
                    self._notify_listeners(event)

                    self.logger.exception('Job "%s" raised an exception', job)
                else:
                    # Notify listeners about successful execution
                    event = JobEvent(EVENT_JOB_EXECUTED, job, run_time, retval=retval)
                    self._notify_listeners(event)

                    self.logger.info('Job "%s" executed successfully', job)

                job.remove_instance()

                # If coalescing is enabled, don't attempt any further runs
                if job.coalesce:
                    break

    def _process_jobs(self):
        """Iterates through jobs in every jobstore, starts jobs that are due and figures out how long to wait for
        the next round.
        """

        self.logger.debug('Looking for jobs to run')
        now = self._current_time()
        next_wakeup_time = None

        with self._jobstores_lock:
            for alias, jobstore in iteritems(self._jobstores):
                jobs, jobstore_next_wakeup_time = jobstore.get_pending_jobs(now)
                if not next_wakeup_time:
                    next_wakeup_time = jobstore_next_wakeup_time
                elif jobstore_next_wakeup_time:
                    next_wakeup_time = min(next_wakeup_time or jobstore_next_wakeup_time)

                for job in jobs:
                    run_times = job.get_run_times(now)
                    if run_times:
                        self._threadpool.submit(self._run_job, job, run_times)

                        # Increase the job's run count
                        job.runs += 1 if job.coalesce else len(run_times)

                        # Update the job, but don't keep finished jobs around
                        if job.compute_next_run_time(now + timedelta(microseconds=1)):
                            jobstore.modify_job(job.id, {'next_run_time': job.next_run_time})
                        else:
                            self._remove_job(job, alias, jobstore)

                    if not next_wakeup_time:
                        next_wakeup_time = job.next_run_time
                    elif job.next_run_time:
                        next_wakeup_time = min(next_wakeup_time, job.next_run_time)

        # Determine the delay until this method should be called again
        if next_wakeup_time is not None:
            wait_seconds = time_difference(next_wakeup_time, now)
            self.logger.debug('Next wakeup is due at %s (in %f seconds)', next_wakeup_time, wait_seconds)
        else:
            wait_seconds = None
            self.logger.debug('No jobs; waiting until a job is added')

        return wait_seconds
