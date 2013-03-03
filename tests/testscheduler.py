from datetime import datetime, timedelta
from logging import StreamHandler, ERROR
from copy import copy
import os

from nose.tools import eq_, raises

from apscheduler.jobstores.ram_store import RAMJobStore
from apscheduler.scheduler import Scheduler, SchedulerAlreadyRunningError
from apscheduler.job import Job
from apscheduler.events import (EVENT_JOB_EXECUTED, SchedulerEvent, EVENT_SCHEDULER_START, EVENT_SCHEDULER_SHUTDOWN,
                                EVENT_JOB_MISSED)
from apscheduler import scheduler

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO


class TestOfflineScheduler(object):
    def setup(self):
        self.scheduler = Scheduler()

    def teardown(self):
        if self.scheduler.running:
            self.scheduler.shutdown()

    @raises(KeyError)
    def test_jobstore_twice(self):
        self.scheduler.add_jobstore(RAMJobStore(), 'dummy')
        self.scheduler.add_jobstore(RAMJobStore(), 'dummy')

    def test_add_tentative_job(self):
        job = self.scheduler.add_date_job(lambda: None, datetime(2200, 7, 24), jobstore='dummy')
        assert isinstance(job, Job)
        eq_(self.scheduler.get_jobs(), [])

    def test_add_job_by_reference(self):
        job = self.scheduler.add_date_job('copy:copy', datetime(2200, 7, 24))
        eq_(job.func, copy)
        eq_(job.func_ref, 'copy:copy')

    def test_configure_jobstore(self):
        conf = {'apscheduler.jobstore.ramstore.class': 'apscheduler.jobstores.ram_store:RAMJobStore'}
        self.scheduler.configure(conf)
        self.scheduler.remove_jobstore('ramstore')

    def test_shutdown_offline(self):
        self.scheduler.shutdown()

    def test_configure_no_prefix(self):
        global_options = {'misfire_grace_time': '2', 'daemonic': 'false'}
        self.scheduler.configure(global_options)
        eq_(self.scheduler.misfire_grace_time, 1)
        eq_(self.scheduler.daemonic, True)

    def test_configure_prefix(self):
        global_options = {'apscheduler.misfire_grace_time': 2, 'apscheduler.daemonic': False}
        self.scheduler.configure(global_options)
        eq_(self.scheduler.misfire_grace_time, 2)
        eq_(self.scheduler.daemonic, False)

    def test_add_listener(self):
        val = []
        self.scheduler.add_listener(val.append)

        event = SchedulerEvent(EVENT_SCHEDULER_START)
        self.scheduler._notify_listeners(event)
        eq_(len(val), 1)
        eq_(val[0], event)

        event = SchedulerEvent(EVENT_SCHEDULER_SHUTDOWN)
        self.scheduler._notify_listeners(event)
        eq_(len(val), 2)
        eq_(val[1], event)

        self.scheduler.remove_listener(val.append)
        self.scheduler._notify_listeners(event)
        eq_(len(val), 2)

    def test_pending_jobs(self):
        # Tests that pending jobs are properly added to the jobs list when
        # the scheduler is started (and not before!)
        self.scheduler.add_date_job(lambda: None, datetime(9999, 9, 9))
        eq_(self.scheduler.get_jobs(), [])

        self.scheduler.start()
        jobs = self.scheduler.get_jobs()
        eq_(len(jobs), 1)


class FakeThread(object):
    def isAlive(self):
        return True


class FakeThreadPool(object):
    def submit(self, func, *args, **kwargs):
        func(*args, **kwargs)


class DummyException(Exception):
    pass


original_now = datetime(2011, 4, 3, 18, 40)


class FakeDateTime(datetime):
    _now = original_now

    @classmethod
    def now(cls):
        return cls._now


class TestJobExecution(object):
    def setup(self):
        self.scheduler = Scheduler(threadpool=FakeThreadPool())
        self.scheduler.add_jobstore(RAMJobStore(), 'default')

        # Make the scheduler think it's running
        self.scheduler._thread = FakeThread()

        self.logstream = StringIO()
        self.loghandler = StreamHandler(self.logstream)
        self.loghandler.setLevel(ERROR)
        scheduler.logger.addHandler(self.loghandler)

    def teardown(self):
        scheduler.logger.removeHandler(self.loghandler)
        if scheduler.datetime == FakeDateTime:
            scheduler.datetime = datetime
        FakeDateTime._now = original_now

    def test_job_name(self):
        def my_job():
            pass

        job = self.scheduler.add_interval_job(my_job, start_date=datetime(2010, 5, 19))
        eq_(repr(job),
            '<Job (name=my_job, trigger=<IntervalTrigger (interval=datetime.timedelta(0, 1), '
            'start_date=datetime.datetime(2010, 5, 19, 0, 0))>)>')

    def test_schedule_object(self):
        # Tests that any callable object is accepted (and not just functions)
        class A:
            def __init__(self):
                self.val = 0

            def __call__(self):
                self.val += 1

        a = A()
        job = self.scheduler.add_interval_job(a, seconds=1)
        self.scheduler._process_jobs(job.next_run_time)
        self.scheduler._process_jobs(job.next_run_time)
        eq_(a.val, 2)

    def test_schedule_method(self):
        # Tests that bound methods can be scheduled (at least with RAMJobStore)
        class A:
            def __init__(self):
                self.val = 0

            def method(self):
                self.val += 1

        a = A()
        job = self.scheduler.add_interval_job(a.method, seconds=1)
        self.scheduler._process_jobs(job.next_run_time)
        self.scheduler._process_jobs(job.next_run_time)
        eq_(a.val, 2)

    def test_unschedule_job(self):
        def increment():
            vals[0] += 1

        vals = [0]
        job = self.scheduler.add_cron_job(increment)
        self.scheduler._process_jobs(job.next_run_time)
        eq_(vals[0], 1)
        self.scheduler.unschedule_job(job)
        self.scheduler._process_jobs(job.next_run_time)
        eq_(vals[0], 1)

    def test_unschedule_func(self):
        def increment():
            vals[0] += 1

        def increment2():
            vals[0] += 1

        vals = [0]
        job1 = self.scheduler.add_cron_job(increment)
        job2 = self.scheduler.add_cron_job(increment2)
        job3 = self.scheduler.add_cron_job(increment)
        eq_(self.scheduler.get_jobs(), [job1, job2, job3])

        self.scheduler.unschedule_func(increment)
        eq_(self.scheduler.get_jobs(), [job2])

    @raises(KeyError)
    def test_unschedule_func_notfound(self):
        self.scheduler.unschedule_func(copy)

    def test_job_finished(self):
        def increment():
            vals[0] += 1

        vals = [0]
        job = self.scheduler.add_interval_job(increment, max_runs=1)
        self.scheduler._process_jobs(job.next_run_time)
        eq_(vals, [1])
        assert job not in self.scheduler.get_jobs()

    def test_job_exception(self):
        def failure():
            raise DummyException

        job = self.scheduler.add_date_job(failure, datetime(9999, 9, 9))
        self.scheduler._process_jobs(job.next_run_time)
        assert 'DummyException' in self.logstream.getvalue()

    def test_misfire_grace_time(self):
        self.scheduler.misfire_grace_time = 3
        job = self.scheduler.add_interval_job(lambda: None, seconds=1)
        eq_(job.misfire_grace_time, 3)

        job = self.scheduler.add_interval_job(lambda: None, seconds=1, misfire_grace_time=2)
        eq_(job.misfire_grace_time, 2)

    def test_coalesce_on(self):
        # Makes sure that the job is only executed once when it is scheduled
        # to be executed twice in a row
        def increment():
            vals[0] += 1

        vals = [0]
        events = []
        scheduler.datetime = FakeDateTime
        self.scheduler.add_listener(events.append, EVENT_JOB_EXECUTED | EVENT_JOB_MISSED)
        job = self.scheduler.add_interval_job(increment, seconds=1, start_date=FakeDateTime.now(), coalesce=True,
                                              misfire_grace_time=2)

        # Turn the clock 14 seconds forward
        FakeDateTime._now += timedelta(seconds=2)

        self.scheduler._process_jobs(FakeDateTime.now())
        eq_(job.runs, 1)
        eq_(len(events), 1)
        eq_(events[0].code, EVENT_JOB_EXECUTED)
        eq_(vals, [1])

    def test_coalesce_off(self):
        # Makes sure that every scheduled run for the job is executed even
        # when they are in the past (but still within misfire_grace_time)
        def increment():
            vals[0] += 1

        vals = [0]
        events = []
        scheduler.datetime = FakeDateTime
        self.scheduler.add_listener(events.append, EVENT_JOB_EXECUTED | EVENT_JOB_MISSED)
        job = self.scheduler.add_interval_job(increment, seconds=1, start_date=FakeDateTime.now(), coalesce=False,
                                              misfire_grace_time=2)

        # Turn the clock 2 seconds forward
        FakeDateTime._now += timedelta(seconds=2)

        self.scheduler._process_jobs(FakeDateTime.now())
        eq_(job.runs, 3)
        eq_(len(events), 3)
        eq_(events[0].code, EVENT_JOB_EXECUTED)
        eq_(events[1].code, EVENT_JOB_EXECUTED)
        eq_(events[2].code, EVENT_JOB_EXECUTED)
        eq_(vals, [3])

    def test_interval(self):
        def increment(amount):
            vals[0] += amount
            vals[1] += 1

        vals = [0, 0]
        job = self.scheduler.add_interval_job(increment, seconds=1, args=[2])
        self.scheduler._process_jobs(job.next_run_time)
        self.scheduler._process_jobs(job.next_run_time)
        eq_(vals, [4, 2])

    def test_interval_schedule(self):
        @self.scheduler.interval_schedule(seconds=1)
        def increment():
            vals[0] += 1

        vals = [0]
        start = increment.job.next_run_time
        self.scheduler._process_jobs(start)
        self.scheduler._process_jobs(start + timedelta(seconds=1))
        eq_(vals, [2])

    def test_cron(self):
        def increment(amount):
            vals[0] += amount
            vals[1] += 1

        vals = [0, 0]
        job = self.scheduler.add_cron_job(increment, args=[3])
        start = job.next_run_time
        self.scheduler._process_jobs(start)
        eq_(vals, [3, 1])
        self.scheduler._process_jobs(start + timedelta(seconds=1))
        eq_(vals, [6, 2])
        self.scheduler._process_jobs(start + timedelta(seconds=2))
        eq_(vals, [9, 3])

    def test_cron_schedule_1(self):
        @self.scheduler.cron_schedule()
        def increment():
            vals[0] += 1

        vals = [0]
        start = increment.job.next_run_time
        self.scheduler._process_jobs(start)
        self.scheduler._process_jobs(start + timedelta(seconds=1))
        eq_(vals[0], 2)

    def test_cron_schedule_2(self):
        @self.scheduler.cron_schedule(minute='*')
        def increment():
            vals[0] += 1

        vals = [0]
        start = increment.job.next_run_time
        next_run = start + timedelta(seconds=60)
        eq_(increment.job.get_run_times(next_run), [start, next_run])
        self.scheduler._process_jobs(start)
        self.scheduler._process_jobs(next_run)
        eq_(vals[0], 2)

    def test_date(self):
        def append_val(value):
            vals.append(value)

        vals = []
        date = datetime.now() + timedelta(seconds=1)
        self.scheduler.add_date_job(append_val, date, kwargs={'value': 'test'})
        self.scheduler._process_jobs(date)
        eq_(vals, ['test'])

    def test_print_jobs(self):
        out = StringIO()
        self.scheduler.print_jobs(out)
        expected = 'Jobstore default:%s'\
                   '    No scheduled jobs%s' % (os.linesep, os.linesep)
        eq_(out.getvalue(), expected)

        self.scheduler.add_date_job(copy, datetime(2200, 5, 19))
        out = StringIO()
        self.scheduler.print_jobs(out)
        expected = 'Jobstore default:%s    '\
            'copy (trigger: date[2200-05-19 00:00:00], '\
            'next run at: 2200-05-19 00:00:00)%s' % (os.linesep, os.linesep)
        eq_(out.getvalue(), expected)

    def test_jobstore(self):
        self.scheduler.add_jobstore(RAMJobStore(), 'dummy')
        job = self.scheduler.add_date_job(lambda: None, datetime(2200, 7, 24), jobstore='dummy')
        eq_(self.scheduler.get_jobs(), [job])
        self.scheduler.remove_jobstore('dummy')
        eq_(self.scheduler.get_jobs(), [])

    @raises(KeyError)
    def test_remove_nonexistent_jobstore(self):
        self.scheduler.remove_jobstore('dummy2')

    def test_job_next_run_time(self):
        # Tests against bug #5
        def increment():
            vars[0] += 1

        vars = [0]
        scheduler.datetime = FakeDateTime
        job = self.scheduler.add_interval_job(
            increment, seconds=1, misfire_grace_time=3,
            start_date=FakeDateTime.now())
        start = job.next_run_time

        self.scheduler._process_jobs(start)
        eq_(vars, [1])

        self.scheduler._process_jobs(start)
        eq_(vars, [1])

        self.scheduler._process_jobs(start + timedelta(seconds=1))
        eq_(vars, [2])


class TestRunningScheduler(object):
    def setup(self):
        self.scheduler = Scheduler()
        self.scheduler.start()

    def teardown(self):
        if self.scheduler.running:
            self.scheduler.shutdown()

    def test_shutdown_timeout(self):
        self.scheduler.shutdown()

    @raises(SchedulerAlreadyRunningError)
    def test_scheduler_double_start(self):
        self.scheduler.start()

    @raises(SchedulerAlreadyRunningError)
    def test_scheduler_configure_running(self):
        self.scheduler.configure({})

    def test_scheduler_double_shutdown(self):
        self.scheduler.shutdown()
        self.scheduler.shutdown(False)
