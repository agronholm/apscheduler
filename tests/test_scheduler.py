from datetime import datetime, timedelta
from logging import StreamHandler, ERROR
from io import StringIO
from copy import copy
import os

from dateutil.tz import tzoffset
import pytest

from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.scheduler import Scheduler, SchedulerAlreadyRunningError
from apscheduler.job import Job
from apscheduler.events import (EVENT_JOB_EXECUTED, SchedulerEvent, EVENT_SCHEDULER_START, EVENT_SCHEDULER_SHUTDOWN,
                                EVENT_JOB_MISSED)
from apscheduler import scheduler

local_tz = tzoffset('DUMMYTZ', 3600)


class FakeThread(object):
    def isAlive(self):
        return True


class FakeThreadPool(object):
    def submit(self, func, *args, **kwargs):
        func(*args, **kwargs)


class DummyException(Exception):
    pass


class FakeDateTime(datetime):
    original_now = datetime(2011, 4, 3, 18, 40, tzinfo=local_tz)
    _now = original_now

    @classmethod
    def now(cls, timezone=None):
        return cls._now


class TestOfflineScheduler(object):
    @pytest.fixture()
    def sched(self, request):
        def finish():
            if sched.running:
                sched.shutdown()

        sched = Scheduler()
        request.addfinalizer(finish)
        return sched

    def test_jobstore_twice(self, sched):
        with pytest.raises(KeyError):
            sched.add_jobstore(MemoryJobStore(), 'dummy')
            sched.add_jobstore(MemoryJobStore(), 'dummy')

    def test_add_tentative_job(self, sched):
        job = sched.add_job(lambda: None, 'date', [datetime(2200, 7, 24)], jobstore='dummy')
        assert isinstance(job, Job)
        assert sched.get_jobs() == []

    def test_add_job_by_reference(self, sched):
        job = sched.add_job('copy:copy', 'date', [datetime(2200, 7, 24)])
        assert job.func == copy
        assert job.func_ref == 'copy:copy'

    def test_configure_jobstore(self, sched):
        conf = {'apscheduler.jobstore.memstore.class': 'apscheduler.jobstores.memory:MemoryJobStore'}
        sched.configure(conf)
        sched.remove_jobstore('memstore')

    def test_shutdown_offline(self, sched):
        sched.shutdown()

    def test_configure_no_prefix(self, sched):
        global_options = {'misfire_grace_time': '2', 'daemonic': 'false'}
        sched.configure(global_options)
        assert sched.misfire_grace_time == 1
        assert sched.daemonic is True

    def test_configure_prefix(self, sched):
        global_options = {'apscheduler.misfire_grace_time': 2, 'apscheduler.daemonic': False}
        sched.configure(global_options)
        assert sched.misfire_grace_time == 2
        assert sched.daemonic == False

    def test_add_listener(self, sched):
        val = []
        sched.add_listener(val.append)

        event = SchedulerEvent(EVENT_SCHEDULER_START)
        sched._notify_listeners(event)
        assert len(val) == 1
        assert val[0] == event

        event = SchedulerEvent(EVENT_SCHEDULER_SHUTDOWN)
        sched._notify_listeners(event)
        assert len(val) == 2
        assert val[1] == event

        sched.remove_listener(val.append)
        sched._notify_listeners(event)
        assert len(val) == 2

    def test_pending_jobs(self, sched):
        # Tests that pending jobs are properly added to the jobs list when
        # the scheduler is started (and not before!)
        sched.add_job(lambda: None, 'date', [datetime(9999, 9, 9)])
        assert sched.get_jobs() == []

        sched.start()
        jobs = sched.get_jobs()
        assert len(jobs) == 1


class TestJobExecution(object):
    @pytest.fixture
    def sched(self):
        sched = Scheduler(threadpool=FakeThreadPool(), timezone=local_tz)
        sched.add_jobstore(MemoryJobStore(), 'default')

        # Make the scheduler think it's running
        sched._thread = FakeThread()
        return sched

    @pytest.fixture
    def logstream(self, request):
        stream = StringIO()
        loghandler = StreamHandler(stream)
        loghandler.setLevel(ERROR)
        scheduler.logger.addHandler(loghandler)
        request.addfinalizer(lambda: scheduler.logger.removeHandler(loghandler))
        return stream

    @pytest.fixture
    def fake_datetime(self, request, monkeypatch):
        monkeypatch.setattr(scheduler, 'datetime', FakeDateTime)
        request.addfinalizer(lambda: setattr(FakeDateTime, '_now', FakeDateTime.original_now))

    def test_job_name(self, sched):
        def my_job():
            pass

        job = sched.add_job(my_job, 'interval', {'start_date': datetime(2010, 5, 19)})
        assert (repr(job) ==
            "<Job (name=my_job, trigger=<IntervalTrigger (interval=datetime.timedelta(0, 1), "
            "start_date='2010-05-19 00:00:00 DUMMYTZ')>)>")

    def test_schedule_object(self, sched):
        # Tests that any callable object is accepted (and not just functions)
        class A:
            def __init__(self):
                self.val = 0

            def __call__(self):
                self.val += 1

        a = A()
        job = sched.add_job(a, 'interval', {'seconds': 1})
        sched._process_jobs(job.next_run_time)
        sched._process_jobs(job.next_run_time)
        assert a.val == 2

    def test_schedule_method(self, sched):
        # Tests that bound methods can be scheduled (at least with RAMJobStore)
        class A:
            def __init__(self):
                self.val = 0

            def method(self):
                self.val += 1

        a = A()
        job = sched.add_job(a.method, 'interval', {'seconds': 1})
        sched._process_jobs(job.next_run_time)
        sched._process_jobs(job.next_run_time)
        assert a.val == 2

    def test_unschedule_job(self, sched):
        def increment():
            vals[0] += 1

        vals = [0]
        job = sched.add_job(increment, 'cron')
        sched._process_jobs(job.next_run_time)
        assert vals[0] == 1
        sched.unschedule_job(job)
        sched._process_jobs(job.next_run_time)
        assert vals[0] == 1

    def test_unschedule_func(self, sched):
        def increment():
            vals[0] += 1

        def increment2():
            vals[0] += 1

        vals = [0]
        job1 = sched.add_job(increment, 'cron')
        job2 = sched.add_job(increment2, 'cron')
        job3 = sched.add_job(increment, 'cron')
        assert sched.get_jobs() == [job1, job2, job3]

        sched.unschedule_func(increment)
        assert sched.get_jobs() == [job2]

    def test_unschedule_func_notfound(self, sched):
        pytest.raises(KeyError, sched.unschedule_func, copy)

    def test_job_finished(self, sched):
        def increment():
            vals[0] += 1

        vals = [0]
        job = sched.add_job(increment, 'interval', max_runs=1)
        sched._process_jobs(job.next_run_time)
        assert vals == [1]
        assert job not in sched.get_jobs()

    def test_job_exception(self, sched, logstream):
        def failure():
            raise DummyException

        job = sched.add_job(failure, 'date', [datetime(9999, 9, 9)])
        sched._process_jobs(job.next_run_time)
        assert 'DummyException' in logstream.getvalue()

    def test_misfire_grace_time(self, sched):
        sched.misfire_grace_time = 3
        job = sched.add_job(lambda: None, 'interval', {'seconds': 1})
        assert job.misfire_grace_time == 3

        job = sched.add_job(lambda: None, 'interval', {'seconds': 1}, misfire_grace_time=2)
        assert job.misfire_grace_time == 2

    def test_coalesce_on(self, sched, fake_datetime):
        # Makes sure that the job is only executed once when it is scheduled
        # to be executed twice in a row
        def increment():
            vals[0] += 1

        vals = [0]
        events = []
        sched.add_listener(events.append, EVENT_JOB_EXECUTED | EVENT_JOB_MISSED)
        job = sched.add_job(increment, 'interval', {'seconds': 1, 'start_date': FakeDateTime.now()},
                                     coalesce=True, misfire_grace_time=2)

        # Turn the clock 14 seconds forward
        FakeDateTime._now += timedelta(seconds=2)

        sched._process_jobs(FakeDateTime.now())
        assert job.runs == 1
        assert len(events) == 1
        assert events[0].code == EVENT_JOB_EXECUTED
        assert vals == [1]

    def test_coalesce_off(self, sched, fake_datetime):
        # Makes sure that every scheduled run for the job is executed even
        # when they are in the past (but still within misfire_grace_time)
        def increment():
            vals[0] += 1

        vals = [0]
        events = []
        sched.add_listener(events.append, EVENT_JOB_EXECUTED | EVENT_JOB_MISSED)
        job = sched.add_job(increment, 'interval', {'seconds': 1, 'start_date': FakeDateTime.now()},
                                     coalesce=False, misfire_grace_time=2)

        # Turn the clock 2 seconds forward
        FakeDateTime._now += timedelta(seconds=2)

        sched._process_jobs(FakeDateTime.now())
        assert job.runs == 3
        assert len(events) == 3
        assert events[0].code == EVENT_JOB_EXECUTED
        assert events[1].code == EVENT_JOB_EXECUTED
        assert events[2].code == EVENT_JOB_EXECUTED
        assert vals == [3]

    def test_interval(self, sched):
        def increment(amount):
            vals[0] += amount
            vals[1] += 1

        vals = [0, 0]
        job = sched.add_job(increment, 'interval', {'seconds': 1}, args=[2])
        sched._process_jobs(job.next_run_time)
        sched._process_jobs(job.next_run_time)
        assert vals == [4, 2]

    def test_interval_schedule(self, sched):
        @sched.scheduled_job('interval', {'seconds': 1})
        def increment():
            vals[0] += 1

        vals = [0]
        start = increment.job.next_run_time
        sched._process_jobs(start)
        sched._process_jobs(start + timedelta(seconds=1))
        assert vals == [2]

    def test_cron(self, sched):
        def increment(amount):
            vals[0] += amount
            vals[1] += 1

        vals = [0, 0]
        job = sched.add_job(increment, 'cron', args=[3])
        start = job.next_run_time
        sched._process_jobs(start)
        assert vals == [3, 1]
        sched._process_jobs(start + timedelta(seconds=1))
        assert vals == [6, 2]
        sched._process_jobs(start + timedelta(seconds=2))
        assert vals == [9, 3]

    def test_cron_schedule_1(self, sched):
        @sched.scheduled_job('cron')
        def increment():
            vals[0] += 1

        vals = [0]
        start = increment.job.next_run_time
        sched._process_jobs(start)
        sched._process_jobs(start + timedelta(seconds=1))
        assert vals[0] == 2

    def test_cron_schedule_2(self, sched):
        @sched.scheduled_job('cron', {'minute': '*'})
        def increment():
            vals[0] += 1

        vals = [0]
        start = increment.job.next_run_time
        next_run = start + timedelta(seconds=60)
        assert increment.job.get_run_times(next_run) == [start, next_run]
        sched._process_jobs(start)
        sched._process_jobs(next_run)
        assert vals[0] == 2

    def test_date(self, sched):
        def append_val(value):
            vals.append(value)

        vals = []
        date = datetime.now(local_tz) + timedelta(seconds=1)
        sched.add_job(append_val, 'date', [date], kwargs={'value': 'test'})
        sched._process_jobs(date)
        assert vals == ['test']

    def test_print_jobs(self, sched):
        out = StringIO()
        sched.print_jobs(out)
        expected = 'Jobstore default:%s'\
                   '    No scheduled jobs%s' % (os.linesep, os.linesep)
        assert out.getvalue() == expected

        sched.add_job(copy, 'date', [datetime(2200, 5, 19)])
        out = StringIO()
        sched.print_jobs(out)
        expected = 'Jobstore default:%s    '\
            'copy (trigger: date[2200-05-19 00:00:00 DUMMYTZ], '\
            'next run at: 2200-05-19 00:00:00 DUMMYTZ)%s' % (os.linesep, os.linesep)
        assert out.getvalue() == expected

    def test_jobstore(self, sched):
        sched.add_jobstore(MemoryJobStore(), 'dummy')
        job = sched.add_job(lambda: None, 'date', [datetime(2200, 7, 24)], jobstore='dummy')
        assert sched.get_jobs() == [job]
        sched.remove_jobstore('dummy')
        assert sched.get_jobs() == []

    def test_remove_nonexistent_jobstore(self, sched):
        pytest.raises(KeyError, sched.remove_jobstore, 'dummy2')

    def test_job_next_run_time(self, sched):
        # Tests against bug #5
        def increment():
            vars[0] += 1

        vars = [0]
        scheduler.datetime = FakeDateTime
        job = sched.add_job(increment, 'interval', {'seconds': 1, 'start_date': FakeDateTime.now()},
                                     misfire_grace_time=3)
        start = job.next_run_time

        sched._process_jobs(start)
        assert vars == [1]

        sched._process_jobs(start)
        assert vars == [1]

        sched._process_jobs(start + timedelta(seconds=1))
        assert vars == [2]


class TestRunningScheduler(object):
    @pytest.fixture
    def sched(self, request):
        sched = Scheduler()
        sched.start()
        request.addfinalizer(sched.shutdown)
        return sched

    def test_shutdown_timeout(self, sched):
        sched.shutdown()

    def test_scheduler_double_start(self, sched):
        pytest.raises(SchedulerAlreadyRunningError, sched.start)

    def test_scheduler_configure_running(self, sched):
        pytest.raises(SchedulerAlreadyRunningError, sched.configure, {})

    def test_scheduler_double_shutdown(self, sched):
        sched.shutdown()
        sched.shutdown(False)
