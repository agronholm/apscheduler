from datetime import datetime, timedelta
from time import sleep
from copy import copy
from cStringIO import StringIO
import os

from nose.tools import eq_, raises

from apscheduler.scheduler import Scheduler, SchedulerShutdownError
from apscheduler.scheduler import SchedulerAlreadyRunningError
from apscheduler.jobstore.ram_store import RAMJobStore


class TestException(Exception):
    pass


class TestRunningScheduler(object):
    def setUp(self):
        self.scheduler = Scheduler()
        self.scheduler.start()

    def tearDown(self):
        if not self.scheduler.stopped:
            self.scheduler.shutdown()

    @raises(TypeError)
    def test_noncallable(self):
        date = datetime.now() + timedelta(days=1)
        self.scheduler.add_date_job('wontwork', date)

    def test_job_name(self):
        def my_job():
            pass

        job = self.scheduler.add_interval_job(my_job,
                                              start_date=datetime(2010, 5, 19))
        eq_(repr(job),
            'apschedulertests.testscheduler.my_job: '
            'IntervalTrigger(interval=datetime.timedelta(0, 1), '
            'start_date=datetime.datetime(2010, 5, 19, 0, 0))')

    def test_interval(self):
        def increment(vals, amount):
            vals[0] += amount
            vals[1] += 1

        vals = [0, 0]
        self.scheduler.add_interval_job(increment, seconds=1, args=[vals, 2])
        sleep(2.2)
        eq_(vals, [4, 2])

    def test_overlapping_runs(self):
        # Makes sure that "increment" is only ran once, since it will still be
        # running when the next appointed time hits.
        def increment(vals):
            vals[0] += 1
            sleep(2)

        vals = [0]
        self.scheduler.add_interval_job(increment, seconds=1, args=[vals])
        sleep(2.2)
        eq_(vals, [1])

    def test_schedule_object(self):
        """
        Tests that any callable object is accepted (and not just functions).
        """
        class A:
            def __init__(self):
                self.val = 0
            def __call__(self):
                self.val += 1

        a = A()
        self.scheduler.add_interval_job(a, seconds=1)
        sleep(2.2)
        eq_(a.val, 2)

    def test_unschedule_job(self):
        def increment(vals):
            vals[0] += 1

        vals = [0]
        job = self.scheduler.add_cron_job(increment, args=[vals])
        sleep(1)
        ref_value = vals[0]
        assert ref_value >= 1
        self.scheduler.unschedule_job(job)
        sleep(1.2)
        eq_(vals[0], ref_value)

    def test_job_finished(self):
        def increment(vals):
            vals[0] += 1

        vals = [0]
        job = self.scheduler.add_interval_job(increment, args=[vals])
        sleep(1.2)
        eq_(vals, [1])
        assert job in self.scheduler.get_jobs()

    @raises(TestException)
    def test_job_exception(self):
        def failure():
            raise TestException

        start_date = datetime(9999, 1, 1)
        jobmeta = self.scheduler.add_date_job(failure, start_date)
        jobmeta.job.run()

    def test_interval_schedule(self):
        vals = [0]

        @self.scheduler.interval_schedule(seconds=1, args=[vals])
        def increment(vals):
            vals[0] += 1

        sleep(2.2)
        eq_(vals, [2])

    def test_cron_schedule(self):
        vals = [0]

        @self.scheduler.cron_schedule(args=[vals])
        def increment(vals):
            vals[0] += 1

        sleep(2.2)
        assert vals[0] >= 2

    def test_date(self):
        def append_val(value):
            vals.append(value)

        vals = []
        date = datetime.now() + timedelta(seconds=1)
        self.scheduler.add_date_job(append_val, date, kwargs={'value': 'test'})
        sleep(2.2)
        eq_(vals, ['test'])

    def test_cron(self):
        def increment(vals, amount):
            vals[0] += amount
            vals[1] += 1

        vals = [0, 0]
        self.scheduler.add_cron_job(increment, args=[vals, 3])
        sleep(3)
        assert vals[0] >= 6
        assert vals[1] >= 3

    def test_shutdown_timeout(self):
        self.scheduler.shutdown(3)

    @raises(SchedulerAlreadyRunningError)
    def test_scheduler_double_start(self):
        self.scheduler.start()

    def test_scheduler_double_shutdown(self):
        self.scheduler.shutdown(1)
        self.scheduler.shutdown()

    @raises(SchedulerShutdownError)
    def test_shutdown_add_job(self):
        """
        Makes sure that the scheduler doesn't accept new jobs after
        it's been shut down.
        """
        self.scheduler.shutdown()
        self.scheduler.add_interval_job(lambda: 1)

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
            'copy.copy: DateTrigger(datetime.datetime(2200, 5, 19, 0, 0)) '\
            '(next fire time: 2200-05-19 00:00:00)' % os.linesep
        eq_(out.getvalue(), expected)


def test_configure_no_prefix():
    global_options = {'misfire_grace_time': '2',
                      'daemonic': 'false'}
    scheduler = Scheduler(global_options, misfire_grace_time=9)
    eq_(scheduler.misfire_grace_time, 9)
    eq_(scheduler.daemonic, True)


def test_configure_prefix():
    global_options = {'apscheduler.misfire_grace_time': 2,
                      'apscheduler.daemonic': False}
    scheduler = Scheduler(global_options)
    eq_(scheduler.misfire_grace_time, 2)
    eq_(scheduler.daemonic, False)


def test_jobstore():
    scheduler = Scheduler()
    scheduler.add_jobstore(RAMJobStore(), 'dummy')
    job = scheduler.add_date_job(lambda: None, datetime(2200, 7, 24),
                                 jobstore='dummy')
    eq_(job.jobstore.alias, 'dummy')
    eq_(scheduler.get_jobs(), [job])
    scheduler.remove_jobstore('dummy')
    eq_(scheduler.get_jobs(), [])
