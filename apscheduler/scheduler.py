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
from apscheduler.job import SimpleJob, JobMeta
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
        self._jobstores_lock = Lock()
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
        threadpool_opts = combine_opts(config, 'threadpool.')
        self._threadpool = ThreadPool(**threadpool_opts)

        # Configure job stores
        self._jobstores = {}
        jobstore_opts = combine_opts(config, 'jobstore.')
        for key, value in jobstore_opts.items():
            store_name, option = key.split('.', 1)[0]
            opts_dict = self._jobstores.setdefault(store_name, {})
            opts_dict[option] = value

        for alias, opts in self._jobstores.items():
            classname = opts.pop('class')
            cls = ref_to_obj(classname)
            jobstore = cls(**opts)
            jobstore.alias = alias
            self._jobstores[alias] = jobstore

        # Create a RAMJobStore as the default if there is no default job store
        if not 'default' in self._jobstores:
            jobstore = RAMJobStore()
            jobstore.alias = 'default'
            self._jobstores['default'] = jobstore

    def start(self):
        """
        Starts the scheduler in a new thread.
        """
        if self.running:
            raise SchedulerAlreadyRunningError

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

    def add_jobstore(self, jobstore, alias=None):
        """
        Adds a job store to this scheduler.

        :param jobstore: job store to be added
        :param alias: alias for the job store (job store's default used if no
            value specified)
        :type jobstore: instance of :class:`~apscheduler.jobstore.base.JobStore`
        :type alias: str
        """
        if hasattr(jobstore, '__call__'):
            jobstore = jobstore()
        jobstore.alias = alias or jobstore.default_alias

        self._jobstores_lock.acquire()
        try:
            if jobstore.alias in self._jobstores:
                raise KeyError('Alias "%s" is already in use' % jobstore.alias)
            self._jobstores[jobstore.alias] = jobstore
        finally:
            self._jobstores_lock.release()

        # Notify the scheduler so it can scan the new job store for jobs
        self._wakeup.set()

    def remove_jobstore(self, alias):
        """
        Removes the jobstore by the given alias from this scheduler.

        :type alias: str
        """
        self._jobstores_lock.acquire()
        try:
            try:
                del self._jobstores[alias]
            except KeyError:
                raise KeyError('No such jobstore: %s' % alias)
        finally:
            self._jobstores_lock.release()

    def _select_jobstore(self, persistent):
        for jobstore in self._jobstores.values():
            if persistent and jobstore.stores_persistent:
                return jobstore
            if not persistent and jobstore.stores_transient:
                return jobstore

        raise Exception('No suitable job store found')

    def add_job(self, job, trigger, persistent=False, jobstore=None,
                **options):
        """
        Adds the given job to the job list and notifies the scheduler thread.

        :param persistent: ``True`` to store the job in the first default
            persistent job store, ``False`` to store it in a transient store
        :param jobstore: alias of the job store to store the job in (overrides
            the ``persistent`` option)
        :return: scheduling metadata for the job if the scheduler is running,
            else ``None``
        :rtype: :class:`~apscheduler.job.JobMeta`
        """
        if not self.running:
            pending = ((job, trigger, persistent, jobstore), (options))
            self._pending_jobs.append(pending)
            jobname = options.get('name', '(unnamed)')
            logger.info('Adding job "%s" tentatively -- job will be scheduled '
                        'when the scheduler starts', jobname)
            return

        jobmeta = JobMeta(job, trigger, **options)
        if jobmeta.misfire_grace_time is None:
            jobmeta.misfire_grace_time = self.misfire_grace_time

        jobmeta.next_run_time = trigger.get_next_fire_time(datetime.now())
        if not jobmeta.next_run_time:
            raise ValueError('Not adding job since it would never be run')

        if jobstore:
            try:
                jobstore = self._jobstores[jobstore]
            except KeyError:
                raise KeyError('No such jobstore: %s' % jobstore)
        else:
            jobstore = self._select_jobstore(persistent)

        jobstore.add_job(jobmeta)
        logger.info('Added job "%s" to job store %s', jobmeta, jobstore.alias)

        # Notify the scheduler about the new job
        self._wakeup.set()

        return jobmeta

    def add_simple_job(self, trigger, func, args, kwargs, name=None,
                       **options):
        """
        Schedules a "simple" job that executes the given function with the
        given positional and keyword arguments.
        
        :param trigger: the trigger instance that controls the execution
            schedule
        :param args: positional arguments with which ``func`` is executed
        :param kwargs: keyword arguments with which ``func`` is executed
        :param name: optional name for the job
        """
        job = SimpleJob(func, args, kwargs)
        name = name or get_callable_name(func)
        return self.add_job(job, trigger, name=name, **options)

    def add_date_job(self, func, date, args=None, kwargs=None, **options):
        """
        Schedules a job to be completed on a specific date and time.

        :param func: callable to run at the given time
        :param date: the date/time to run the job at
        :param name: name of the job
        :param persistent: ``True`` to store the job in any persistent job store
        :param jobstore: stored the job in the named (or given) job store
        :param misfire_grace_time: seconds after the designated run time that
            the job is still allowed to be run
        :type date: :class:`datetime.date`
        :return: the scheduled job
        :rtype: :class:`~apscheduler.job.JobMeta`
        """
        trigger = SimpleTrigger(date)
        return self.add_simple_job(trigger, func, args, kwargs, **options)

    def add_interval_job(self, func, weeks=0, days=0, hours=0, minutes=0,
                         seconds=0, start_date=None, repeat=0, args=None,
                         kwargs=None):
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
        :param repeat: number of times the job will be run (0 = repeat
            indefinitely)
        :param args: list of positional arguments to call func with
        :param kwargs: dict of keyword arguments to call func with
        :param name: name of the job
        :param persistent: ``True`` to store the job in any persistent job store
        :param jobstore: alias of the job store to add the job to
        :param misfire_grace_time: seconds after the designated run time that
            the job is still allowed to be run
        :return: the scheduled job
        :rtype: :class:`~apscheduler.job.JobMeta`
        """
        interval = timedelta(weeks=weeks, days=days, hours=hours,
                             minutes=minutes, seconds=seconds)
        trigger = IntervalTrigger(interval, repeat, start_date)
        return self.add_simple_job(trigger, func, args, kwargs)

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
        :param persistent: ``True`` to store the job in any persistent job store
        :param jobstore: alias of the job store to add the job to
        :param misfire_grace_time: seconds after the designated run time that
            the job is still allowed to be run
        :return: the scheduled job
        :rtype: :class:`~apscheduler.job.JobMeta`
        """
        trigger = CronTrigger(year=year, month=month, day=day, week=week,
                              day_of_week=day_of_week, hour=hour,
                              minute=minute, second=second,
                              start_date=start_date)
        return self.add_simple_job(trigger, func, args, kwargs, **options)

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
        Note that the default repeat value is 0, which means to repeat forever.
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
            jobs.extend(jobstore.list_jobs())
        return jobs

    def unschedule_job(self, job):
        """
        Removes a job, preventing it from being fired any more.
        """
        job.jobstore.remove_job(job)
        logger.info('Removed job "%s"', job)
        self._wakeup.set()

    def print_jobs(self, out=sys.stdout):
        """
        Prints out a textual listing of all jobs currently scheduled on this
        scheduler.

        :param out: a file-like object to print to.
        """
        job_strs = []
        for alias, jobstore in self._jobstores.items():
            job_strs.append('Jobstore %s:' % alias)
            jobmetas = jobstore.list_jobs()
            if jobmetas:
                for jobmeta in jobmetas:
                    job_str = '    %s (next fire time: %s)'
                    job_str %= (jobmeta, jobmeta.next_run_time)
                    job_strs.append(job_str)
            else:
                job_strs.append('    No scheduled jobs')

        out.write(os.linesep.join(job_strs))

    @staticmethod
    def _run_job(jobmeta):
        """
        Acts as a harness that runs the actual job code in a thread.
        """
        logger.debug('Running job "%s"', jobmeta)
        try:
            jobmeta.job.run()
        except:
            logger.exception('Job "%s" raised an exception', jobmeta)
        else:
            logger.debug('Finished job "%s"', jobmeta)

        jobmeta.jobstore.checkin_job(jobmeta)

    def _start_job(self, jobmeta):
        # See if the job missed its run time window, and handle possible
        # misfires accordingly
        difference = datetime.now() - jobmeta.next_run_time
        grace_time = timedelta(seconds=jobmeta.misfire_grace_time)
        if difference > grace_time:
            late = difference - grace_time
            logger.error('Run time of job "%s" (%s) was missed by %s',
                         jobmeta, late)
            # TODO: handle misfire
            return

        self._threadpool.execute(self._run_job, [jobmeta])

    def _main_loop(self):
        """
        Adds any pending jobs to the scheduler and then runs the the main loop.
        """
        self._wakeup.clear()
        for args, options in self._pending_jobs:
            try:
                self.add_job(*args, **options)
            except:
                pass
        del self._pending_jobs[:]

        while not self._stopped:
            # Iterate through pending jobs in every jobstore, start them
            # and figure out the next wakeup time
            logger.debug('Scanning jobstores for jobs to run')
            end_time = datetime.now()
            next_wakeup_time = None
            for jobstore in self._jobstores.values():
                jobmetas = jobstore.checkout_jobs(end_time)
                for jobmeta in jobmetas:
                    self._start_job(jobmeta)

                next_run_time = jobstore.get_next_run_time(end_time)
                if next_run_time and (not next_wakeup_time or
                                      next_run_time < next_wakeup_time):
                    next_wakeup_time = next_run_time

            # Sleep until the next job is scheduled to be run,
            # a new job is added or the scheduler is stopped
            if next_wakeup_time is not None:
                now = datetime.now()
                wait_seconds = time_difference(next_wakeup_time, now)
                logger.debug('Next wakeup is due at %s (in %f seconds)',
                             next_wakeup_time, wait_seconds)
                self._wakeup.wait(wait_seconds)
            else:
                logger.debug('No jobs; waiting until a job is added')
                self._wakeup.wait()
            self._wakeup.clear()
