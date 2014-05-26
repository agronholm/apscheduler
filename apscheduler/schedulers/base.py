from __future__ import print_function
from abc import ABCMeta, abstractmethod
from threading import RLock
from datetime import datetime, timedelta
from logging import getLogger
import sys

from tzlocal import get_localzone
import six

from apscheduler.schedulers import SchedulerAlreadyRunningError, SchedulerNotRunningError
from apscheduler.executors.base import MaxInstancesReachedError
from apscheduler.executors.pool import PoolExecutor
from apscheduler.jobstores.base import ConflictingIdError
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.job import Job
from apscheduler.util import combine_opts, maybe_ref, asbool, astimezone, timedelta_seconds, undefined, asint
from apscheduler.events import (
    SchedulerEvent, JobStoreEvent, EVENT_SCHEDULER_START, EVENT_SCHEDULER_SHUTDOWN, EVENT_JOBSTORE_ADDED,
    EVENT_JOBSTORE_REMOVED, EVENT_ALL, EVENT_JOBSTORE_JOB_MODIFIED, EVENT_JOBSTORE_JOB_REMOVED,
    EVENT_JOBSTORE_JOB_ADDED)


class BaseScheduler(six.with_metaclass(ABCMeta)):
    """Base class for all schedulers."""

    _stopped = True

    #
    # Public API
    #

    def __init__(self, gconfig={}, **options):
        super(BaseScheduler, self).__init__()
        self._executors = {}
        self._executors_lock = self._create_lock()
        self._jobstores = {}
        self._jobstores_lock = self._create_lock()
        self._listeners = []
        self._listeners_lock = self._create_lock()
        self._pending_jobs = []
        self.configure(gconfig, **options)

    def configure(self, gconfig={}, **options):
        """
        Reconfigures the scheduler with the given options. Can only be done when the scheduler isn't running.

        :param dict gconfig: a "global" configuration dictionary whose values can be overridden by keyword arguments to
                             this method
        :raises SchedulerAlreadyRunningError: if the scheduler is already running
        """

        if self.running:
            raise SchedulerAlreadyRunningError

        config = combine_opts(gconfig, 'apscheduler.', options)
        self._configure(config)

    @abstractmethod
    def start(self):
        """
        Starts the scheduler. The details of this process depend on the implementation.

        :raises SchedulerAlreadyRunningError: if the scheduler is already running
        """

        if self.running:
            raise SchedulerAlreadyRunningError

        with self._executors_lock:
            # Create a default executor if nothing else is configured
            if 'default' not in self._executors:
                self.add_executor(self._create_default_executor(), 'default')

            # Start all the executors
            for alias, executor in six.iteritems(self._executors):
                executor.start(self, alias)

        with self._jobstores_lock:
            # Create a default job store if nothing else is configured
            if 'default' not in self._jobstores:
                self.add_jobstore(self._create_default_jobstore(), 'default')

            # Start all the job stores
            for alias, store in six.iteritems(self._executors):
                store.start(self, alias)

            # Schedule all pending jobs
            for job, jobstore, replace_existing in self._pending_jobs:
                self._real_add_job(job, replace_existing, False)
            del self._pending_jobs[:]

        self._stopped = False
        self._logger.info('Scheduler started')

        # Notify listeners that the scheduler has been started
        self._dispatch_event(SchedulerEvent(EVENT_SCHEDULER_START))

    @abstractmethod
    def shutdown(self, wait=True):
        """
        Shuts down the scheduler. Does not interrupt any currently running jobs.

        :param bool wait: ``True`` to wait until all currently executing jobs have finished
        :raises SchedulerNotRunningError: if the scheduler has not been started yet
        """

        if not self.running:
            raise SchedulerNotRunningError

        self._stopped = True

        # Shut down all executors
        for executor in six.itervalues(self._executors):
            executor.shutdown(wait)

        # Shut down all job stores
        for jobstore in six.itervalues(self._jobstores):
            jobstore.shutdown()

        self._logger.info('Scheduler has been shut down')
        self._dispatch_event(SchedulerEvent(EVENT_SCHEDULER_SHUTDOWN))

    @property
    def running(self):
        return not self._stopped

    def add_executor(self, executor, alias='default'):
        """
        Adds an executor to this scheduler.

        :param apscheduler.executors.base.BaseExecutor executor: the executor instance to be added
        :param str|unicode alias: alias for the scheduler
        """

        with self._executors_lock:
            if alias in self._executors:
                raise KeyError('This scheduler already has an executor by the alias of "%s"' % alias)
            executor.scheduler = self
            self._executors[alias] = executor

            # Start the executor right away if the scheduler is running
            if self.running:
                executor.start(self)

    def add_jobstore(self, jobstore, alias='default'):
        """
        Adds a job store to this scheduler.

        :param apscheduler.jobstores.base.BaseJobStore jobstore: job store to be added
        :param str|unicode alias: alias for the job store
        """

        with self._jobstores_lock:
            if alias in self._jobstores:
                raise KeyError('This scheduler already has a job store by the alias of "%s"' % alias)
            self._jobstores[alias] = jobstore

            # Start the job store right away if the scheduler is running
            if self.running:
                jobstore.start(self, alias)

        # Notify listeners that a new job store has been added
        self._dispatch_event(JobStoreEvent(EVENT_JOBSTORE_ADDED, alias))

        # Notify the scheduler so it can scan the new job store for jobs
        if self.running:
            self._wakeup()

    def remove_jobstore(self, alias, shutdown=True):
        """
        Removes the job store by the given alias from this scheduler.

        :param str|unicode alias: alias of the job store
        :param bool shutdown: ``True`` to shut down the job store after removing it
        """

        with self._jobstores_lock:
            jobstore = self._lookup_jobstore(alias)
            del self._jobstores[alias]

        # Shut down the job store if requested
        if shutdown:
            jobstore.shutdown()

        # Notify listeners that a job store has been removed
        self._dispatch_event(JobStoreEvent(EVENT_JOBSTORE_REMOVED, alias))

    def add_listener(self, callback, mask=EVENT_ALL):
        """
        Adds a listener for scheduler events. When a matching event occurs, ``callback`` is executed with the event
        object as its sole argument. If the ``mask`` parameter is not provided, the callback will receive events of all
        types.

        :param callback: any callable that takes one argument
        :param int mask: bitmask that indicates which events should be listened to
        """

        with self._listeners_lock:
            self._listeners.append((callback, mask))

    def remove_listener(self, callback):
        """Removes a previously added event listener."""

        with self._listeners_lock:
            for i, (cb, _) in enumerate(self._listeners):
                if callback == cb:
                    del self._listeners[i]

    def add_job(self, func, trigger=None, args=None, kwargs=None, id=None, name=None, misfire_grace_time=undefined,
                coalesce=undefined, max_runs=undefined, max_instances=undefined, jobstore='default', executor='default',
                replace_existing=False, **trigger_args):
        """
        Adds the given job to the job list and notifies the scheduler thread.

        The ``func`` argument can be given either as a callable object or a textual reference in the
        ``package.module:some.object`` format, where the first half (separated by ``:``) is an importable module and the
        second half is a reference to the callable object, relative to the module.

        The ``trigger`` argument can either be:

        # the plugin name of the trigger (e.g. "cron"), in which case any extra keyword arguments to this method are
          passed on to the trigger's constructor
        # an instance of a trigger class

        :param func: callable (or a textual reference to one) to run at the given time
        :param str|apscheduler.triggers.base.BaseTrigger trigger: trigger that determines when ``func`` is called
        :param list|tuple args: list of positional arguments to call func with
        :param dict kwargs: dict of keyword arguments to call func with
        :param str|unicode id: explicit identifier for the job (for modifying it later)
        :param str|unicode name: textual description of the job
        :param int misfire_grace_time: seconds after the designated run time that the job is still allowed to be run
        :param bool coalesce: run once instead of many times if the scheduler determines that the job should be run more
                              than once in succession
        :param int max_runs: maximum number of times this job is allowed to be triggered
        :param int max_instances: maximum number of concurrently running instances allowed for this job
        :param str|unicode jobstore: alias of the job store to store the job in
        :param str|unicode executor: alias of the executor to run the job with
        :param bool replace_existing: ``True`` to replace an existing job with the same ``id`` (but retain the
                                      number of runs from the existing one)
        :rtype: Job
        """

        # Use the scheduler's time zone if nothing else is specified
        trigger_args.setdefault('timezone', self.timezone)

        # If no trigger was specified, assume that the job should be run now
        if trigger is None:
            trigger = 'date'
            misfire_grace_time = None

        # Assemble the final job arguments, substituting with default values where no other value is provided
        default_replace = lambda key, value: value if value is not undefined else self._job_defaults.get(key)
        job_kwargs = {
            'trigger': trigger,
            'trigger_args': trigger_args,
            'executor': executor,
            'func': func,
            'args': tuple(args) if args is not None else (),
            'kwargs': dict(kwargs) if kwargs is not None else {},
            'id': id,
            'name': name,
            'misfire_grace_time': default_replace('misfire_grace_time', misfire_grace_time),
            'coalesce': default_replace('coalesce', coalesce),
            'max_runs': default_replace('max_runs', max_runs),
            'max_instances': default_replace('max_instances', max_instances),
        }
        job = Job(self, jobstore, **job_kwargs)

        # Don't really add jobs to job stores before the scheduler is up and running
        with self._jobstores_lock:
            if not self.running:
                self._pending_jobs.append((job, jobstore, replace_existing))
                self._logger.info('Adding job tentatively -- it will be properly scheduled when the scheduler starts')
            else:
                self._real_add_job(job, replace_existing, True)

        return job

    def scheduled_job(self, trigger, args=None, kwargs=None, id=None, name=None, misfire_grace_time=None, coalesce=None,
                      max_runs=None, max_instances=1, jobstore='default', executor='default', **trigger_args):
        """A decorator version of :meth:`add_job`."""

        def inner(func):
            self.add_job(func, trigger, args, kwargs, id, misfire_grace_time, coalesce, name, max_runs, max_instances,
                         jobstore, executor, True, **trigger_args)
            return func
        return inner

    def modify_job(self, job_id, jobstore='default', **changes):
        """
        Modifies the properties of a single job. Modifications are passed to this method as extra keyword arguments.

        :param str|unicode job_id: the identifier of the job
        :param str|unicode jobstore: alias of the job store that contains the job
        """

        with self._jobstores_lock:
            # Check if the job is among the pending jobs
            for job, store, replace_existing in self._pending_jobs:
                if job.id == job_id:
                    job._modify(**changes)
                    return

            # Otherwise, look up the job store, make the modifications to the job and have the store update it
            store = self._lookup_jobstore(jobstore)
            job = store.lookup_job(changes.get('id', job_id))
            job._modify(**changes)
            store.update_job(job)

        self._dispatch_event(JobStoreEvent(EVENT_JOBSTORE_JOB_MODIFIED, jobstore, job_id))

        # Wake up the scheduler since the job's next run time may have been changed
        self._wakeup()

    def pause_job(self, job_id, jobstore='default'):
        """
        Causes the given job not to be executed until it is explicitly resumed.

        :param str|unicode job_id: the identifier of the job
        :param str|unicode jobstore: alias of the job store that contains the job
        """

        self.modify_job(job_id, jobstore, next_run_time=None)

    def resume_job(self, job_id, jobstore='default'):
        """
        Resumes the schedule of the given job, or removes the job if its schedule is finished.

        :param str|unicode job_id: the identifier of the job
        :param str|unicode jobstore: alias of the job store that contains the job
        """

        with self._jobstores_lock:
            store = self._lookup_jobstore(jobstore)
            job = store.lookup_job(job_id)
            now = datetime.now(self.timezone)
            next_run_time = job.trigger.get_next_fire_time(now)
            if next_run_time:
                self.modify_job(job_id, jobstore, next_run_time=next_run_time)
            else:
                self.remove_job(job.id, jobstore)

    def get_jobs(self, jobstore=None, pending=None):
        """
        Returns a list of pending jobs (if the scheduler hasn't been started yet) and scheduled jobs,
        either from a specific job store or from all of them.

        :param str|unicode jobstore: alias of the job store
        :param bool pending: ``False`` to leave out pending jobs (jobs that are waiting for the scheduler start to be
                             added to their respective job stores), ``True`` to only include pending jobs, anything else
                             to return both
        :rtype: list[Job]
        """

        with self._jobstores_lock:
            jobs = []

            if pending is not False:
                for job, alias, replace_existing in self._pending_jobs:
                    if jobstore is None or alias == jobstore:
                        jobs.append(job)

            if pending is not True:
                jobstores = {jobstore: self._lookup_jobstore(jobstore)} if jobstore else self._jobstores
                for alias, store in six.iteritems(jobstores):
                    for job in store.get_all_jobs():
                        jobs.append(job)

            return jobs

    def get_job(self, job_id, jobstore='default'):
        """
        Returns the Job that matches the given ``job_id``.

        :param str|unicode job_id: the identifier of the job
        :param str|unicode jobstore: alias of the job store that contains the job
        :rtype: Job
        """

        with self._jobstores_lock:
            store = self._lookup_jobstore(jobstore)
            return store.lookup_job(job_id)

    def remove_job(self, job_id, jobstore='default'):
        """
        Removes a job, preventing it from being run any more.

        :param str|unicode job_id: the identifier of the job
        :param str|unicode jobstore: alias of the job store that contains the job
        """

        with self._jobstores_lock:
            # Check if the job is among the pending jobs
            for i, (job, store, replace_existing) in enumerate(self._pending_jobs):
                if job.id == job_id:
                    del self._pending_jobs[i]
                    return

            store = self._lookup_jobstore(jobstore)
            store.remove_job(job_id)

        # Notify listeners that a job has been removed
        event = JobStoreEvent(EVENT_JOBSTORE_JOB_REMOVED, jobstore, job_id)
        self._dispatch_event(event)

        self._logger.info('Removed job %s', job_id)

    def remove_all_jobs(self, jobstore=None):
        """
        Removes all jobs from the specified job store, or all job stores if none is given.

        :param str|unicode jobstore: alias of the job store
        """

        with self._jobstores_lock:
            jobstores = {jobstore: self._lookup_jobstore(jobstore)} if jobstore else self._jobstores
            for store in six.itervalues(jobstores):
                store.remove_all_jobs()

    def print_jobs(self, jobstore=None, out=None):
        """
        Prints out a textual listing of all jobs currently scheduled on either all job stores or just a specific one.

        :param str|unicode jobstore: alias of the job store, ``None`` to list jobs from all stores
        :param file out: a file-like object to print to (defaults to **sys.stdout** if nothing is given)
        """

        out = out or sys.stdout
        with self._jobstores_lock:
            jobs = self.get_jobs(jobstore, True)
            if jobs:
                print(six.u('Pending jobs:'), file=out)
                for job in jobs:
                    print(six.u('    %s') % job, file=out)

            for alias, store in six.iteritems(self._jobstores):
                if jobstore is None or alias == jobstore:
                    print(six.u('Jobstore %s:') % alias, file=out)
                    jobs = self.get_jobs(jobstore, False)
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
        self._logger = maybe_ref(config.pop('logger', None)) or getLogger('apscheduler.scheduler')
        self.timezone = astimezone(config.pop('timezone', None)) or get_localzone()

        # Set the job defaults
        job_defaults = combine_opts(config, 'job_defaults.')
        self._job_defaults = {
            'misfire_grace_time': asint(job_defaults.get('misfire_grace_time', '1')),
            'coalesce': asbool(job_defaults.get('coalesce', True)),
            'max_runs': asint(job_defaults.get('max_runs')),
            'max_instances': asint(job_defaults.get('max_instances', '1'))
        }

        # Configure executors
        executor_opts = combine_opts(config, 'executor.')
        executors = {}
        for key, value in executor_opts.items():
            store_name, option = key.split('.', 1)
            opts_dict = executors.setdefault(store_name, {})
            opts_dict[option] = value

        for alias, opts in executors.items():
            classname = opts.pop('class')
            cls = maybe_ref(classname)
            executor = cls(**opts)
            self.add_executor(executor, alias)

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
            self.add_jobstore(jobstore, alias)

    def _create_default_executor(self):
        """Creates a default executor store, specific to the particular scheduler type."""

        return PoolExecutor('thread')

    def _create_default_jobstore(self):
        """Creates a default job store, specific to the particular scheduler type."""

        return MemoryJobStore()

    def _lookup_executor(self, alias):
        """
        Returns the executor instance by the given name from the list of executors that were added to this scheduler.

        :type alias: str
        :raises KeyError: if no executor by the given alias is not found
        """

        try:
            return self._executors[alias]
        except KeyError:
            raise KeyError('No such executor: %s' % alias)

    def _lookup_jobstore(self, alias):
        """
        Returns the job store instance by the given name from the list of job stores that were added to this scheduler.

        :type alias: str
        :raises KeyError: if no job store by the given alias is not found
        """

        try:
            return self._jobstores[alias]
        except KeyError:
            raise KeyError('No such job store: %s' % alias)

    def _dispatch_event(self, event):
        """
        Dispatches the given event to interested listeners.

        :param SchedulerEvent event: the event to send
        """

        with self._listeners_lock:
            listeners = tuple(self._listeners)

        for cb, mask in listeners:
            if event.code & mask:
                try:
                    cb(event)
                except:
                    self._logger.exception('Error notifying listener')

    def _real_add_job(self, job, replace_existing, wakeup):
        """
        :param Job job: the job to add
        :param bool replace_existing: ``True`` to use update_job() in case the job already exists in the store
        :param bool wakeup: ``True`` to wake up the scheduler after adding the job
        """

        # Calculate the next run time
        now = datetime.now(self.timezone)
        job.next_run_time = job.trigger.get_next_fire_time(now)

        # Add the job to the given job store
        jobstore_alias = job._jobstore
        store = self._lookup_jobstore(jobstore_alias)
        try:
            store.add_job(job)
        except ConflictingIdError:
            if replace_existing:
                existing_job = store.lookup_job(job.id)
                job.runs = existing_job.runs
                store.update(job)
            else:
                raise

        # Mark the job as no longer pending
        job._jobstore = store

        # Notify listeners that a new job has been added
        event = JobStoreEvent(EVENT_JOBSTORE_JOB_ADDED, job.id, jobstore_alias)
        self._dispatch_event(event)

        self._logger.info('Added job "%s" to job store "%s"', job.name, jobstore_alias)

        # Notify the scheduler about the new job
        if wakeup:
            self._wakeup()

    @abstractmethod
    def _wakeup(self):
        """Triggers :meth:`_process_jobs` to be run in an implementation specific manner."""

    def _create_lock(self):
        """Creates a reentrant lock object."""

        return RLock()

    def _process_jobs(self):
        """
        Iterates through jobs in every jobstore, starts jobs that are due and figures out how long to wait for the next
        round.
        """

        self._logger.debug('Looking for jobs to run')
        now = datetime.now(self.timezone)
        next_wakeup_time = None

        with self._jobstores_lock:
            for jobstore_alias, jobstore in six.iteritems(self._jobstores):
                for job in jobstore.get_due_jobs(now):
                    # Look up the job's executor
                    try:
                        executor = self._lookup_executor(job.executor)
                    except:
                        self._logger.error('Executor lookup ("%s") failed for job "%s" -- removing it from the job'
                                           'store', job.executor, job)
                        self.remove_job(job.id, jobstore_alias)
                        continue

                    run_times = job._get_run_times(now)
                    run_times = run_times[-1:] if run_times and job.coalesce else run_times
                    if run_times:
                        try:
                            executor.submit_job(job, run_times)
                        except MaxInstancesReachedError:
                            self._logger.warning(
                                'Execution of job "%s" skipped: maximum number of running instances reached (%d)',
                                job, job.max_instances)
                            continue
                        except:
                            self._logger.exception('Error submitting job "%s" to executor "%s"', job, job.executor)
                            continue

                        # Update the job if it has a next execution time and the number of runs has not reached maximum,
                        # otherwise remove it from the job store
                        job_runs = job.runs + len(run_times)
                        job_next_run = job.trigger.get_next_fire_time(now + timedelta(microseconds=1))
                        if job_next_run and (job.max_runs is None or job_runs < job.max_runs):
                            job._modify(next_run_time=job_next_run, runs=job_runs)
                            jobstore.update_job(job)
                        else:
                            self.remove_job(job.id, jobstore_alias)

                # Set a new next wakeup time if there isn't one yet or the jobstore has an even earlier one
                jobstore_next_run_time = jobstore.get_next_run_time()
                if jobstore_next_run_time and (next_wakeup_time is None or jobstore_next_run_time < next_wakeup_time):
                    next_wakeup_time = jobstore_next_run_time

        # Determine the delay until this method should be called again
        if next_wakeup_time is not None:
            wait_seconds = max(timedelta_seconds(next_wakeup_time - now), 0)
            self._logger.debug('Next wakeup is due at %s (in %f seconds)', next_wakeup_time, wait_seconds)
        else:
            wait_seconds = None
            self._logger.debug('No jobs; waiting until a job is added')

        return wait_seconds
