"""
This module is the main part of the library. It houses the Scheduler class
and related exceptions.
"""

from threading import Thread, Event, Lock
from datetime import datetime, timedelta
from logging import getLogger
import os
import sys

from apscheduler.util import *
from apscheduler.triggers import SimpleTrigger, IntervalTrigger, CronTrigger
from apscheduler.jobstores.ram_store import RAMJobStore
from apscheduler.job import *
from apscheduler.threadpool import ThreadPool


logger = getLogger(__name__)

class SchedulerAlreadyRunningError(Exception):
    """
    Raised when attempting to start or configure the scheduler when it's
    already running.
    """

    def __str__(self):
        return 'Scheduler is already running'


class Scheduler(object):
    """
    This class is responsible for scheduling jobs and triggering
    their execution.
    """

    _stopped = False
    _thread = None

    def __init__(self, gconfig={}, **options):
        self._wakeup = Event()
        self._jobstores = {}
        self._jobstores_lock = Lock()
        self._listeners = []
        self._listeners_lock = Lock()
        self._pending_jobs = []
        self.configure(gconfig, **options)

    def configure(self, gconfig={}, **options):
        """
        Reconfigures the scheduler with the given options. Can only be done
        when the scheduler isn't running.
        """
        if self.running:
            raise SchedulerAlreadyRunningError

        # Set general options
        config = combine_opts(gconfig, 'apscheduler.', options)
        self.misfire_grace_time = int(config.pop('misfire_grace_time', 1))
        self.daemonic = asbool(config.pop('daemonic', True))

        # Configure the thread pool
        if 'threadpool' in config:
            self._threadpool = maybe_ref(config['threadpool'])
        else:
            threadpool_opts = combine_opts(config, 'threadpool.')
            self._threadpool = ThreadPool(**threadpool_opts)

        # Configure job stores
        jobstore_opts = combine_opts(config, 'jobstore.')
        for key, value in jobstore_opts.items():
            store_name, option = key.split('.', 1)[0]
            opts_dict = self._jobstores.setdefault(store_name, {})
            opts_dict[option] = value

        for alias, opts in self._jobstores.items():
            classname = opts.pop('class')
            cls = maybe_ref(classname)
            jobstore = cls(**opts)
            self.add_jobstore(jobstore, alias, True)

    def start(self):
        """
        Starts the scheduler in a new thread.
        """
        if self.running:
            raise SchedulerAlreadyRunningError

        # Create a RAMJobStore as the default if there is no default job store
        if not 'default' in self._jobstores:
            self.add_jobstore(RAMJobStore(), 'default', True)

        # Add pending jobs as actual jobs
        for trigger, func, args, kwargs, jobstore, opts in self._pending_jobs:
            self.add_job(trigger, func, args, kwargs, jobstore, True, **opts)
        del self._pending_jobs[:]

        self._stopped = False
        self._thread = Thread(target=self._main_loop, name='APScheduler')
        self._thread.setDaemon(self.daemonic)
        self._thread.start()
        logger.info('Scheduler started')

    def shutdown(self, timeout=0):
        """
        Shuts down the scheduler and terminates the thread.
        Does not terminate any currently running jobs.

        :param timeout: time (in seconds) to wait for the scheduler thread to
            terminate, ``0`` to wait forever, ``None`` to skip waiting
        """
        if not self.running:
            return

        logger.info('Scheduler shutting down')
        self._stopped = True
        self._wakeup.set()
        if timeout is not None:
            self._thread.join(timeout)

    @property
    def running(self):
        return not self._stopped and self._thread and self._thread.isAlive()

    def add_jobstore(self, jobstore, alias=None, quiet=False):
        """
        Adds a job store to this scheduler.

        :param jobstore: job store to be added
        :param alias: alias for the job store (job store's default used if no
            value specified)
        :type jobstore: instance of
            :class:`~apscheduler.jobstores.base.JobStore`
        :type alias: str
        """
        jobstore.alias = alias or jobstore.default_alias

        self._jobstores_lock.acquire()
        try:
            if jobstore.alias in self._jobstores:
                raise KeyError('Alias "%s" is already in use' % jobstore.alias)
            self._jobstores[jobstore.alias] = jobstore
            jobstore.load_jobs()
        finally:
            self._jobstores_lock.release()

        # Notify the scheduler so it can scan the new job store for jobs
        if not quiet:
            self._wakeup.set()

    def remove_jobstore(self, alias):
        """
        Removes the job store by the given alias from this scheduler.

        :type alias: str
        """
        self._jobstores_lock.acquire()
        try:
            try:
                del self._jobstores[alias]
            except KeyError:
                raise KeyError('No such job store: %s' % alias)
        finally:
            self._jobstores_lock.release()

    def add_listener(self, callback, mask):
        self._listeners_lock.acquire()
        try:
            self._listeners.append((callback, mask))
        finally:
            self._listeners_lock.release()

    def remove_listener(self, callback):
        self._listeners_lock.acquire()
        try:
            for i, (cb, _) in enumerate(self._listeners):
                if callback == cb:
                    del self._listeners[i]
        finally:
            self._listeners_lock.release()

    def _notify_listeners(self, status):
        for cb, mask in tuple(self._listeners):
            if status.code & mask:
                try:
                    cb(status)
                except:
                    logger.exception('Error notifying listener')

    def add_job(self, trigger, func, args, kwargs, jobstore='default',
                quiet=False, **options):
        """
        Adds the given job to the job list and notifies the scheduler thread.

        :param trigger: alias of the job store to store the job in
        :param func: callable to run at the given time
        :param args: list of positional arguments to call func with
        :param kwargs: dict of keyword arguments to call func with
        :param jobstore: alias of the job store to store the job in
        :return: the newly scheduled job if the scheduler is running, else
            ``None``
        :rtype: :class:`~apscheduler.job.Job`
        """
        if not self.running and not quiet:
            args = trigger, func, args, kwargs, jobstore, options
            self._pending_jobs.append(args)
            logger.info('Adding job tentatively -- it will be properly '
                        'scheduled when the scheduler starts')
            return

        job = Job(trigger, func, args or [], kwargs or {}, **options)
        if job.misfire_grace_time is None:
            job.misfire_grace_time = self.misfire_grace_time
        job.next_run_time = trigger.get_next_fire_time(datetime.now())
        if not job.next_run_time:
            raise ValueError('Not adding job since it would never be run')

        try:
            store = self._jobstores[jobstore]
        except KeyError:
            raise KeyError('No such job store: %s' % jobstore)
        store.add_job(job)
        logger.info('Added job "%s" to job store "%s"', job, jobstore)

        # Notify the scheduler about the new job
        if not quiet:
            self._wakeup.set()

        return job

    def add_date_job(self, func, date, args=None, kwargs=None, **options):
        """
        Schedules a job to be completed on a specific date and time.

        :param func: callable to run at the given time
        :param date: the date/time to run the job at
        :param name: name of the job
        :param jobstore: stored the job in the named (or given) job store
        :param misfire_grace_time: seconds after the designated run time that
            the job is still allowed to be run
        :type date: :class:`datetime.date`
        :return: the scheduled job
        :rtype: :class:`~apscheduler.job.Job`
        """
        trigger = SimpleTrigger(date)
        return self.add_job(trigger, func, args, kwargs, **options)

    def add_interval_job(self, func, weeks=0, days=0, hours=0, minutes=0,
                         seconds=0, start_date=None, args=None, kwargs=None,
                         **options):
        """
        Schedules a job to be completed on specified intervals.

        :param func: callable to run
        :param weeks: number of weeks to wait
        :param days: number of days to wait
        :param hours: number of hours to wait
        :param minutes: number of minutes to wait
        :param seconds: number of seconds to wait
        :param start_date: when to first execute the job and start the
            counter (default is after the given interval)
        :param args: list of positional arguments to call func with
        :param kwargs: dict of keyword arguments to call func with
        :param name: name of the job
        :param jobstore: alias of the job store to add the job to
        :param misfire_grace_time: seconds after the designated run time that
            the job is still allowed to be run
        :return: the scheduled job
        :rtype: :class:`~apscheduler.job.Job`
        """
        interval = timedelta(weeks=weeks, days=days, hours=hours,
                             minutes=minutes, seconds=seconds)
        trigger = IntervalTrigger(interval, start_date)
        return self.add_job(trigger, func, args, kwargs, **options)

    def add_cron_job(self, func, year='*', month='*', day='*', week='*',
                     day_of_week='*', hour='*', minute='*', second='*',
                     start_date=None, args=None, kwargs=None, **options):
        """
        Schedules a job to be completed on times that match the given
        expressions.

        :param func: callable to run
        :param year: year to run on
        :param month: month to run on (0 = January)
        :param day: day of month to run on
        :param week: week of the year to run on
        :param day_of_week: weekday to run on (0 = Monday)
        :param hour: hour to run on
        :param second: second to run on
        :param args: list of positional arguments to call func with
        :param kwargs: dict of keyword arguments to call func with
        :param name: name of the job
        :param jobstore: alias of the job store to add the job to
        :param misfire_grace_time: seconds after the designated run time that
            the job is still allowed to be run
        :return: the scheduled job
        :rtype: :class:`~apscheduler.job.Job`
        """
        trigger = CronTrigger(year=year, month=month, day=day, week=week,
                              day_of_week=day_of_week, hour=hour,
                              minute=minute, second=second,
                              start_date=start_date)
        return self.add_job(trigger, func, args, kwargs, **options)

    def cron_schedule(self, **options):
        """
        Decorator that causes its host function to be scheduled
        according to the given parameters.
        This decorator does not wrap its host function.
        The scheduled function will be called without any arguments.
        See :meth:`add_cron_job` for more information.
        """
        def inner(func):
            self.add_cron_job(func, **options)
            return func
        return inner

    def interval_schedule(self, **options):
        """
        Decorator that causes its host function to be scheduled
        for execution on specified intervals.
        This decorator does not wrap its host function.
        The scheduled function will be called without any arguments.
        See :meth:`add_delayed_job` for more information.
        """
        def inner(func):
            self.add_interval_job(func, **options)
            return func
        return inner

    def get_jobs(self):
        """
        Returns a list of all scheduled jobs.
        """
        jobs = []
        for jobstore in self._jobstores.values():
            jobs.extend(jobstore.jobs)
        return jobs

    def unschedule_job(self, job):
        """
        Removes a job, preventing it from being run any more.
        """
        for jobstore in self._jobstores.values():
            if job in jobstore.jobs:
                jobstore.remove_job(job)
                logger.info('Removed job "%s"', job)
                return

        raise ValueError('Job "%s" is not scheduled in any job store' % job)

    def print_jobs(self, out=sys.stdout):
        """
        Prints out a textual listing of all jobs currently scheduled on this
        scheduler.

        :param out: a file-like object to print to.
        """
        job_strs = []
        for alias, jobstore in self._jobstores.items():
            job_strs.append('Jobstore %s:' % alias)
            if jobstore.jobs:
                for job in jobstore.jobs:
                    job_strs.append('    %s' % job)
            else:
                job_strs.append('    No scheduled jobs')

        out.write(os.linesep.join(job_strs))

    def _run_job(self, job, run_time):
        """
        Acts as a harness that runs the actual job code in a thread.
        """
        logger.debug('Running job "%s" (scheduled at %s)', job, run_time)
        status = JobStatus(job, run_time)

        # See if the job missed its run time window, and handle possible
        # misfires accordingly
        difference = datetime.now() - run_time
        grace_time = timedelta(seconds=job.misfire_grace_time)
        if difference > grace_time:
            status.code = STATUS_MISSED
            logger.warning('Run time of job "%s" was missed by %s',
                           job, difference)
        elif job.instances == job.max_concurrency:
            status.code = STATUS_MISSED
            logger.warning('Execution of job "%s" skipped: too many instances '
                           'running already (%d)', job, job.max_concurrency)
        else:
            job.instances -= 1
            try:
                status.retval = job.func(*job.args, **job.kwargs)
            except:
                job.instances -= 1
                job.runs += 1
                status.exception, status.traceback = sys.exc_info()[1:]
                status.code = STATUS_ERROR
                logger.exception('Job "%s" raised an exception', job)
            else:
                job.instances -= 1
                job.runs += 1
                status.code = STATUS_OK
                logger.debug('Job "%s" executed successfully', job)

        if not job.next_run_time:
            status.code |= STATUS_FINISHED
            logger.info('Job "%s" has finished and will no longer be '
                        'executed', job)

        self._notify_listeners(status)

    def _main_loop(self):
        """Executes jobs on schedule."""

        self._wakeup.clear()
        while not self._stopped:
            # Iterate through pending jobs in every jobstore, start them
            # and figure out the next wakeup time
            logger.debug('Looking for jobs to run')
            now = datetime.now()
            next_wakeup_time = None
            for jobstore in self._jobstores.values():
                for job in tuple(jobstore.jobs):
                    run_time = job.next_run_time
                    if run_time <= now:
                        job.runs += 1
                        job.compute_next_run_time(now)
                        self._threadpool.submit(self._run_job, job, run_time)
                    if not next_wakeup_time:
                        next_wakeup_time = job.next_run_time
                    elif job.next_run_time:
                        next_wakeup_time = min(next_wakeup_time,
                                               job.next_run_time)
                    jobstore.update_job(job)

            # Sleep until the next job is scheduled to be run,
            # a new job is added or the scheduler is stopped
            if next_wakeup_time is not None:
                wait_seconds = time_difference(next_wakeup_time, now)
                logger.debug('Next wakeup is due at %s (in %f seconds)',
                             next_wakeup_time, wait_seconds)
                self._wakeup.wait(wait_seconds)
            else:
                logger.debug('No jobs; waiting until a job is added')
                self._wakeup.wait()
            self._wakeup.clear()
