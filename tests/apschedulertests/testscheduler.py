from datetime import datetime, timedelta
from copy import copy
import os

from nose.tools import eq_, raises

from apscheduler.jobstores.ram_store import RAMJobStore
from apscheduler.scheduler import Scheduler, SchedulerAlreadyRunningError
from apscheduler.job import STATUS_OK, STATUS_ERROR, JobStatus

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO


class TestOfflineScheduler(object):
    def setup(self):
        self.scheduler = Scheduler()

    @raises(KeyError)
    def test_jobstore_twice(self):
        self.scheduler.add_jobstore(RAMJobStore(), 'dummy')
        self.scheduler.add_jobstore(RAMJobStore(), 'dummy')

    def test_add_tentative_job(self):
        job = self.scheduler.add_date_job(lambda: None, datetime(2200, 7, 24),
                                          jobstore='dummy')
        eq_(job, None)
        eq_(self.scheduler.get_jobs(), [])

    def test_configure_jobstore(self):
        conf = {'apscheduler.jobstore.ramstore.class': 'apscheduler.jobstores.ram_store:RAMJobStore'}
        self.scheduler.configure(conf)
        self.scheduler.remove_jobstore('ramstore')

    def test_shutdown_offline(self):
        self.scheduler.shutdown()

    def test_configure_no_prefix(self):
        global_options = {'misfire_grace_time': '2',
                          'daemonic': 'false'}
        self.scheduler.configure(global_options)
        eq_(self.scheduler.misfire_grace_time, 1)
        eq_(self.scheduler.daemonic, True)
    
    def test_configure_prefix(self):
        global_options = {'apscheduler.misfire_grace_time': 2,
                          'apscheduler.daemonic': False}
        self.scheduler.configure(global_options)
        eq_(self.scheduler.misfire_grace_time, 2)
        eq_(self.scheduler.daemonic, False)

    def test_add_listener(self):
        def cb(status):
            val[0] += 1
        val = [0]
        self.scheduler.add_listener(cb, STATUS_OK)

        status = JobStatus(None, None)
        status.code = STATUS_OK
        self.scheduler._notify_listeners(status)
        eq_(val[0], 1)

        status.code = STATUS_ERROR
        self.scheduler._notify_listeners(status)
        eq_(val[0], 1)

        self.scheduler.remove_listener(cb)
        self.scheduler._notify_listeners(status)
        eq_(val[0], 1)


class FakeThread(object):
    def isAlive(self):
        return True


class FakeThreadPool(object):
    def submit(self, func, *args, **kwargs):
        func(*args, **kwargs)


class DummyException(Exception):
    pass


class TestJobExecution(object):
    def setUp(self):
        self.scheduler = Scheduler(threadpool=FakeThreadPool())
        self.scheduler.add_jobstore(RAMJobStore(), 'default')

        # Make the scheduler think it's running
        self.scheduler._thread = FakeThread()

    @raises(TypeError)
    def test_noncallable(self):
        date = datetime.now() + timedelta(days=1)
        self.scheduler.add_date_job('wontwork', date)

    def test_job_name(self):
        def my_job():
            pass

        job = self.scheduler.add_interval_job(my_job,
                                              start_date=datetime(2010, 5, 19))
        eq_(repr(job), '<Job (name=apschedulertests.testscheduler.my_job, '
            'trigger=<IntervalTrigger (interval=datetime.timedelta(0, 1), '
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

    def test_unschedule_job(self):
        def increment(vals):
            vals[0] += 1

        vals = [0]
        job = self.scheduler.add_cron_job(increment, args=[vals])
        self.scheduler._process_jobs(job.next_run_time)
        eq_(vals[0], 1)
        self.scheduler.unschedule_job(job)
        self.scheduler._process_jobs(job.next_run_time)
        eq_(vals[0], 1)

    def test_job_finished(self):
        def increment():
            vals[0] += 1

        vals = [0]
        job = self.scheduler.add_interval_job(increment, max_runs=1)
        self.scheduler._process_jobs(job.next_run_time)
        eq_(vals, [1])
        assert job not in self.scheduler.get_jobs()

    @raises(DummyException)
    def test_job_exception(self):
        def failure():
            raise DummyException

        start_date = datetime(9999, 1, 1)
        job = self.scheduler.add_date_job(failure, start_date)
        job.func()

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

    def test_cron_schedule(self):
        @self.scheduler.cron_schedule()
        def increment():
            vals[0] += 1

        vals = [0]
        start = increment.job.next_run_time
        self.scheduler._process_jobs(start)
        self.scheduler._process_jobs(start + timedelta(seconds=1))
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
                   '    No scheduled jobs' % os.linesep
        eq_(out.getvalue(), expected)

        self.scheduler.add_date_job(copy, datetime(2200, 5, 19))
        out = StringIO()
        self.scheduler.print_jobs(out)
        expected = 'Jobstore default:%s    '\
            'copy.copy (trigger: date[2200-05-19 00:00:00], '\
            'next run at: 2200-05-19 00:00:00)' % os.linesep
        eq_(out.getvalue(), expected)

    def test_jobstore(self):
        self.scheduler.add_jobstore(RAMJobStore(), 'dummy')
        job = self.scheduler.add_date_job(lambda: None, datetime(2200, 7, 24),
                                          jobstore='dummy')
        eq_(self.scheduler.get_jobs(), [job])
        self.scheduler.remove_jobstore('dummy')
        eq_(self.scheduler.get_jobs(), [])

    def test_job_next_run_time(self):
        # Tests against bug #5
        def job_1():
            vars[0] += 1

        vars = [0]
        job = self.scheduler.add_interval_job(job_1, seconds=1,
                                              misfire_grace_time=3)
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
        self.scheduler.shutdown(3)
    
    @raises(SchedulerAlreadyRunningError)
    def test_scheduler_double_start(self):
        self.scheduler.start()

    @raises(SchedulerAlreadyRunningError)
    def test_scheduler_configure_running(self):
        self.scheduler.configure({})

    def test_scheduler_double_shutdown(self):
        self.scheduler.shutdown(1)
        self.scheduler.shutdown()
