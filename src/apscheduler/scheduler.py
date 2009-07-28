from threading import Thread, Event, RLock
from datetime import datetime, timedelta
from logging import getLogger
import weakref

from apscheduler.util import time_difference
from apscheduler.triggers import *


logger = getLogger(__name__)


class Job(object):
    """
    Represents a tasks scheduled in the scheduler.
    """

    def __init__(self, trigger, func, args, kwargs):
        self.thread = None
        self.trigger = trigger
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.error_callbacks = []
    
    def run(self, daemonic):
        if (self.thread and self.thread.isAlive()):
            logger.debug('Skipping run (previous thread is still running)')
        else:
            self.thread = Thread(target=self.run_func_in_thread)
            self.thread.setDaemon(daemonic)
            self.thread.start()
    
    def run_func_in_thread(self):
        try:
            self.func(*self.args, **self.kwargs)
        except Exception, e:
            logger.error('Error executing job (%s): %s' % (self.func.funcname, e))

    def __str__(self):
        return self.func.__name__

class JobHandle(object):
    def __init__(self, job, scheduler):
        self.job = weakref.proxy(job)
        self.scheduler = weakref.proxy(scheduler)

    def unschedule(self):
        if self.scheduler and self.job:
            self.scheduler._unschedule(self.job)
    

class Scheduler(Thread):
    def __init__(self, **config):
        Thread.__init__(self)
        self.jobs = []
        self.jobs_lock = RLock()
        self.wakeup = Event()
        self.stopped = False
        self.configure(config)
    
    def configure(self, config):
        self.grace_seconds = int(config.get('grace_seconds', 1))
        self.daemonic_jobs = bool(config.get('daemonic_jobs', False))
    
    def shutdown(self):
        """
        Shuts down the scheduler and terminates the thread.
        Does not terminate any currently running jobs.
        """
        self.stopped = True
        self.wakeup.set()

    def cron_schedule(self, year='*', month='*', day='*', day_of_week='*',
                      hour='*', minute='*', second='*'):
        """
        Decorator that causes its host function to be scheduled
        according to the given parameters.
        This decorator does not wrap its host function.
        The scheduled function will be called without any arguments.
        @see: add_cron_job
        """
        def inner(func):
            self.add_cron_job(func, year, month, day, day_of_week, hour,
                              minute, second)
            return func
        return inner

    def interval_schedule(self, weeks=0, days=0, hours=0, minutes=0, seconds=0,
                          start_date=None, repeat=0):
        """
        Decorator that causes its host function to be scheduled
        for execution on specified intervals.
        This decorator does not wrap its host function.
        The scheduled function will be called without any arguments.
        Note that the default repeat value is 0, which means to repeat forever.
        @see: add_delayed_job
        """
        def inner(func):
            self.add_delayed_job(func, weeks, days, hours, minutes, seconds,
                                 start_date, repeat)
            return func
        return inner
    
    def add_job(self, func, date, args=None, kwargs=None):
        """
        Adds a job to be completed on a specific date and time.

        @param func: callable to run
        @param args: positional arguments to call func with
        @param kwargs: keyword arguments to call func with
        """
        if not isinstance(date, datetime):
            raise TypeError('date must be a datetime object')
        
        trigger = DateTrigger(date)
        return self._add_job(trigger, func, args, kwargs)
    
    def add_delayed_job(self, func, weeks=0, days=0, hours=0, minutes=0,
                        seconds=0, start_date=None, repeat=1, args=None,
                        kwargs=None):
        """
        Adds a job to be completed after the specified delay.

        @param func: callable to run
        @param weeks: number of weeks to wait
        @param days: number of days to wait
        @param hours: number of hours to wait
        @param minutes: number of minutes to wait
        @param seconds: number of seconds to wait
        @param start_date: when to first execute the job and start the
            counter (default is after the given interval)
        @param repeat: number of times the job will be run (0 = repeat
            indefinitely)
        @param args: list of positional arguments to call func with
        @param kwargs: dict of keyword arguments to call func with
        """
        interval = timedelta(weeks=weeks, days=days, hours=hours,
                             minutes=minutes, seconds=seconds)
        trigger = IntervalTrigger(interval, repeat, start_date)
        return self._add_job(trigger, func, args, kwargs)

    def add_cron_job(self, func, year='*', month='*', day='*', day_of_week='*',
                     hour='*', minute='*', second='*', args=None, kwargs=None):
        """
        Adds a job to be completed on specified intervals.
        
        The possible syntaxes for calendar fields are:
        '*' (fire on every value)
        '*/a' (fire every a)
        'a' (fire on the specified value)
        a (same as the previous, but given directly as an integer)
        'a-b' (range; a must be smaller than b)
        'a-b/c' stepped range field, fires every c within the a-b range
        'last' (last valid value, only useful for the month field)
        'x,y,z,...' (fire on any matching expression; can combine any of the
        above)

        @param func: callable to run
        @param year: year to run on
        @param month: month to run on (0 = January)
        @param day: day of month to run on
        @param day_of_week: weekday to run on (0 = Monday)
        @param hour: hour to run on
        @param second: second to run on
        @param args: list of positional arguments to call func with
        @param kwargs: dict of keyword arguments to call func with
        @return: a handle to the scheduled job
        @rtype: JobHandle
        """
        trigger = CronTrigger(year, month, day, day_of_week, hour, minute,
                              second)
        self._add_job(trigger, func, args, kwargs)

    def unschedule(self, func):
        """
        Removes all jobs that match that would execute the given function.
        """
        self.jobs_lock.acquire()
        try:
            remove_list = [job for job in self.jobs if job.func == func]
            for job in remove_list:
                self.jobs.remove(job)
        finally:
            self.jobs_lock.release()
        self.wakeup.set()
    
    def _add_job(self, trigger, func, args, kwargs):
        if not hasattr(func, '__call__'):
            raise TypeError('func must be callable')

        if args is None:
            args = []
        if kwargs is None:
            kwargs = {}

        job = Job(trigger, func, args, kwargs)
        self.jobs_lock.acquire()
        try:
            self.jobs.append(job)
        finally:
            self.jobs_lock.release()
        
        # Notify the scheduler about the new job
        self.wakeup.set()

        return JobHandle(job, self)

    def _unschedule(self, job):
        """
        Removes a job, preventing it from being fired any more.
        """
        self.jobs_lock.acquire()
        try:
            self.jobs.remove(job)
        finally:
            self.jobs_lock.release()
        self.wakeup.set()

    def _get_next_wakeup_time(self, now):
        next_wakeup = None

        self.jobs_lock.acquire()
        try:
            for job in self.jobs:
                wakeup = job.trigger.get_next_fire_time(now)
                if wakeup and (next_wakeup is None or wakeup < next_wakeup):
                    next_wakeup = wakeup
        finally:
            self.jobs_lock.release()

        return next_wakeup
    
    def _get_current_jobs(self):
        current_jobs = []
        finished_jobs = []
        now = datetime.now()
        start = now - timedelta(seconds=self.grace_seconds)
        
        self.jobs_lock.acquire()
        try:
            for job in self.jobs:
                next_run = job.trigger.get_next_fire_time(start)
                if next_run is None:
                    # This job will never be run again
                    finished_jobs.append(job)
                else:
                    time_diff = time_difference(now, next_run)
                    if next_run < now and time_diff <= self.grace_seconds:
                        current_jobs.append(job)

            # Clear out any finished jobs
            for job in finished_jobs:
                self.jobs.remove(job)
        finally:
            self.jobs_lock.release()

        return current_jobs
    
    def run(self):
        self.wakeup.clear()
        while not self.stopped:
            # Execute any jobs scheduled to be run right now
            for job in self._get_current_jobs():
                logger.debug('Executing job "%s"' % job)
                job.run(self.daemonic_jobs)

            # Figure out when the next job should be run, and
            # adjust the wait time accordingly
            wait_seconds = None
            now = datetime.now()
            next_wakeup_time = self._get_next_wakeup_time(now)
            if next_wakeup_time is not None:
                wait_seconds = time_difference(next_wakeup_time, now)

            # Sleep until the next job is scheduled to be run,
            # or a new job is added, or the scheduler is stopped
            if wait_seconds is not None:
                logger.debug('Next wakeup is due in %d seconds' % wait_seconds)
            else:
                logger.debug('No jobs; waiting until a job is added')
            self.wakeup.wait(wait_seconds)
            self.wakeup.clear()
            
           