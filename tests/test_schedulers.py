from datetime import datetime, timedelta
from logging import StreamHandler, ERROR
from io import StringIO, BytesIO
from copy import copy
from threading import Event, Thread
from time import sleep
import os

from dateutil.tz import tzoffset
import pytest

from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.schedulers import SchedulerAlreadyRunningError, SchedulerNotRunningError
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.base import BaseScheduler
from apscheduler.events import (SchedulerEvent, EVENT_JOB_EXECUTED, EVENT_SCHEDULER_START, EVENT_SCHEDULER_SHUTDOWN,
                                EVENT_JOB_MISSED, EVENT_JOBSTORE_ADDED, EVENT_JOBSTORE_JOB_ADDED,
                                EVENT_JOBSTORE_JOB_REMOVED)
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.threadpool import ThreadPool
from tests.conftest import minpython

dummy_tz = tzoffset('DUMMYTZ', 3600)
dummy_datetime = datetime(2011, 4, 3, 18, 40, tzinfo=dummy_tz)


class DummyThreadPool(object):
    def submit(self, func, *args, **kwargs):
        func(*args, **kwargs)

    def shutdown(self, wait):
        pass


class DummyException(Exception):
    pass


class DummyScheduler(BaseScheduler):
    def __init__(self, gconfig={}, **options):
        super(DummyScheduler, self).__init__(gconfig, timezone=dummy_tz, threadpool=DummyThreadPool(), **options)
        self.now = dummy_datetime

    def start(self):
        super(DummyScheduler, self).start()

    def shutdown(self, wait=True):
        super(DummyScheduler, self).shutdown()

    def _wakeup(self):
        pass

    def _current_time(self):
        return self.now


def increment(vals):
    vals[0] += 1


@pytest.fixture
def scheduler(request):
    sched = DummyScheduler()
    if 'start_scheduler' in request.keywords:
        sched.start()
        request.addfinalizer(lambda: sched.shutdown() if sched.running else None)

    return sched


@pytest.fixture
def logstream(request, scheduler):
    stream = BytesIO()
    loghandler = StreamHandler(stream)
    loghandler.setLevel(ERROR)
    scheduler.logger.addHandler(loghandler)
    request.addfinalizer(lambda: scheduler.logger.removeHandler(loghandler))
    return stream


class TestOfflineScheduler(object):
    def test_jobstore_twice(self, scheduler):
        with pytest.raises(KeyError):
            scheduler.add_jobstore(MemoryJobStore(), 'dummy')
            scheduler.add_jobstore(MemoryJobStore(), 'dummy')

    def test_add_job_by_reference(self, scheduler):
        job = scheduler.add_job('copy:copy', 'date', [datetime(2200, 7, 24)], args=[()])
        assert job.func == 'copy:copy'

    def test_modify_job_offline(self, scheduler):
        scheduler.add_job(lambda: None, 'interval', {'seconds': 1}, id='foo')
        scheduler.modify_job('foo', max_runs=3456)
        job = scheduler.get_jobs()[0]
        assert job.max_runs == 3456

    def test_configure_jobstore(self, scheduler):
        conf = {'apscheduler.jobstore.memstore.class': 'apscheduler.jobstores.memory:MemoryJobStore'}
        scheduler.configure(conf)
        scheduler.remove_jobstore('memstore')

    def test_shutdown_offline(self, scheduler):
        pytest.raises(SchedulerNotRunningError, scheduler.shutdown)

    def test_configure_no_prefix(self, scheduler):
        global_options = {'misfire_grace_time': '2', 'coalesce': 'false'}
        scheduler.configure(global_options)
        assert scheduler.misfire_grace_time == 1
        assert scheduler.coalesce is True

    def test_configure_prefix(self, scheduler):
        global_options = {'apscheduler.misfire_grace_time': 2, 'apscheduler.coalesce': False}
        scheduler.configure(global_options)
        assert scheduler.misfire_grace_time == 2
        assert scheduler.coalesce is False

    def test_add_listener(self, scheduler):
        val = []
        scheduler.add_listener(val.append)

        event = SchedulerEvent(EVENT_SCHEDULER_START)
        scheduler._notify_listeners(event)
        assert len(val) == 1
        assert val[0] == event

        event = SchedulerEvent(EVENT_SCHEDULER_SHUTDOWN)
        scheduler._notify_listeners(event)
        assert len(val) == 2
        assert val[1] == event

        scheduler.remove_listener(val.append)
        scheduler._notify_listeners(event)
        assert len(val) == 2

    def test_pending_jobs(self, scheduler):
        """Tests that pending jobs are properly added to the jobs list, but only when the scheduler is started."""

        job = scheduler.add_job(lambda: None, 'date', [datetime(9999, 9, 9)])
        assert scheduler.get_jobs(pending=False) == []
        assert scheduler.get_jobs(pending=True) == [job]
        assert scheduler.get_jobs() == [job]

        scheduler.start()
        jobs = scheduler.get_jobs(pending=False)
        assert len(jobs) == 1

    def test_print_pending_jobs(self, scheduler):
        out = StringIO()
        scheduler.print_jobs(out=out)
        assert out.getvalue() == ''

        scheduler.add_job(copy, 'date', [datetime(2200, 5, 19)], args=[()])
        out = StringIO()
        scheduler.print_jobs(out=out)
        expected = 'Pending jobs:%s    '\
            'copy (trigger: date[2200-05-19 00:00:00 DUMMYTZ], '\
            'next run at: 2200-05-19 00:00:00 DUMMYTZ)%s' % (os.linesep, os.linesep)
        assert out.getvalue() == expected

    def test_invalid_callable_args(self, scheduler):
        """Tests that attempting to schedule a job with an invalid number of arguments raises an exception."""

        exc = pytest.raises(ValueError, scheduler.add_job, lambda x: None, 'date', [datetime(9999, 9, 9)], args=[1, 2])
        assert str(exc.value) == ('The list of positional arguments is longer than the target callable can handle '
                                  '(allowed: 1, given in args: 2)')

    def test_invalid_callable_kwargs(self, scheduler):
        """Tests that attempting to schedule a job with unmatched keyword arguments raises an exception."""

        exc = pytest.raises(ValueError, scheduler.add_job, lambda x: None, 'date', [datetime(9999, 9, 9)],
                            kwargs={'x': 0, 'y': 1})
        assert str(exc.value) == 'The target callable does not accept the following keyword arguments: y'

    def test_missing_callable_args(self, scheduler):
        """Tests that attempting to schedule a job with missing arguments raises an exception."""

        exc = pytest.raises(ValueError, scheduler.add_job, lambda x, y, z: None, 'date', [datetime(9999, 9, 9)],
                            args=[1], kwargs={'y': 0})
        assert str(exc.value) == 'The following arguments are not supplied: z'

    def test_conflicting_callable_args(self, scheduler):
        """Tests that attempting to schedule a job where the combination of args and kwargs are in conflict raises an
        exception."""

        exc = pytest.raises(ValueError, scheduler.add_job, lambda x, y: None, 'date', [datetime(9999, 9, 9)],
                            args=[1, 2], kwargs={'y': 1})
        assert str(exc.value) == 'The following arguments are supplied in both args and kwargs: y'

    @minpython(3)
    def test_unfulfilled_kwargs(self, scheduler):
        """Tests that attempting to schedule a job where not all keyword-only arguments are fulfilled raises an
        exception."""

        func = eval("lambda x, *, y, z=1: None")
        exc = pytest.raises(ValueError, scheduler.add_job, func, 'date', [datetime(9999, 9, 9)], args=[1])
        assert str(exc.value) == 'The following keyword-only arguments have not been supplied in kwargs: y'


@pytest.mark.start_scheduler
class TestRunningScheduler(object):
    def test_add_job_object(self, scheduler):
        """Tests that any callable object is accepted (and not just functions)."""

        class A(object):
            def __init__(self):
                self.val = 0

            def __call__(self):
                self.val += 1

        a = A()
        job = scheduler.add_job(a, 'interval', {'seconds': 1})
        scheduler.now = job.next_run_time
        scheduler._process_jobs()
        assert a.val == 1

    def test_add_job_method(self, scheduler):
        """Tests that bound methods can be scheduled (at least with MemoryJobStore)."""

        class A(object):
            def __init__(self):
                self.val = 0

            def method(self):
                self.val += 1

        a = A()
        job = scheduler.add_job(a.method, 'interval', {'seconds': 1})
        scheduler.now = job.next_run_time
        scheduler._process_jobs()
        assert a.val == 1

    def test_add_job_decorator(self, scheduler):
        """Tests that the scheduled_job decorator works."""

        @scheduler.scheduled_job('interval', {'seconds': 1})
        def dummyjob():
            pass

        jobs = scheduler.get_jobs()
        assert len(jobs) == 1
        assert jobs[0].name == 'dummyjob'

    def test_modify_job_online(self, scheduler):
        scheduler.add_job(lambda: None, 'interval', {'seconds': 1}, id='foo')
        scheduler.modify_job('foo', max_runs=3456)
        job = scheduler.get_jobs()[0]
        assert job.max_runs == 3456

    def test_remove_job(self, scheduler):
        vals = [0]
        job = scheduler.add_job(increment, 'cron', args=(vals,))
        scheduler.now = job.next_run_time
        scheduler._process_jobs()
        assert vals[0] == 1

        scheduler.remove_job(job.id)
        scheduler._process_jobs()
        assert vals[0] == 1

    def test_remove_all_jobs(self, scheduler):
        """Tests that removing all jobs clears all job stores."""

        vals = [0]
        scheduler.add_jobstore(MemoryJobStore(), 'alter')
        scheduler.add_job(increment, 'interval', {'seconds': 1}, args=(vals,))
        scheduler.add_job(increment, 'interval', {'seconds': 1}, args=(vals,), jobstore='alter')
        scheduler.remove_all_jobs()
        assert scheduler.get_jobs() == []

    def test_remove_all_jobs_specific_jobstore(self, scheduler):
        """Tests that removing all jobs from a specific job store does not affect the rest."""

        vals = [0]
        scheduler.add_jobstore(MemoryJobStore(), 'alter')
        scheduler.add_job(increment, 'interval', {'seconds': 1}, args=(vals,))
        job2 = scheduler.add_job(increment, 'interval', {'seconds': 1}, args=(vals,), jobstore='alter')
        scheduler.remove_all_jobs('default')
        assert scheduler.get_jobs() == [job2]

    def test_job_finished(self, scheduler):
        vals = [0]
        job = scheduler.add_job(increment, 'interval', args=(vals,), max_runs=1)
        scheduler.now = job.next_run_time
        scheduler._process_jobs()
        assert vals == [1]
        assert job not in scheduler.get_jobs()

    def test_job_exception(self, scheduler, logstream):
        def failure():
            raise DummyException

        job = scheduler.add_job(failure, 'date', [datetime(9999, 9, 9)])
        scheduler.now = job.next_run_time
        scheduler._process_jobs()
        assert 'DummyException' in logstream.getvalue()

    def test_misfire_grace_time(self, scheduler):
        scheduler.misfire_grace_time = 3
        job = scheduler.add_job(lambda: None, 'interval', {'seconds': 1})
        assert job.misfire_grace_time == 3

        job = scheduler.add_job(lambda: None, 'interval', {'seconds': 1}, misfire_grace_time=2)
        assert job.misfire_grace_time == 2

    def test_coalesce_on(self, scheduler):
        """Tests that the job is only executed once when it is scheduled to be executed twice in a row."""

        vals = [0]
        events = []
        scheduler.add_listener(events.append, EVENT_JOB_EXECUTED | EVENT_JOB_MISSED)
        job = scheduler.add_job(increment, 'interval', {'seconds': 1, 'start_date': dummy_datetime},
                                args=(vals,), coalesce=True, misfire_grace_time=2)

        # Turn the clock 2 seconds forward
        scheduler.now += timedelta(seconds=2)

        scheduler._process_jobs()
        job.refresh()
        assert job.runs == 1
        assert len(events) == 1
        assert events[0].code == EVENT_JOB_EXECUTED
        assert vals == [1]

    def test_coalesce_off(self, scheduler):
        """Tests that every scheduled run for the job is executed even when they are in the past
        (but still within misfire_grace_time).
        """

        vals = [0]
        events = []
        scheduler.add_listener(events.append, EVENT_JOB_EXECUTED | EVENT_JOB_MISSED)
        job = scheduler.add_job(increment, 'interval', {'seconds': 1, 'start_date': dummy_datetime},
                                args=(vals,), coalesce=False, misfire_grace_time=2)

        # Turn the clock 2 seconds forward
        scheduler.now += timedelta(seconds=2)

        scheduler._process_jobs()
        job.refresh()
        assert job.runs == 3
        assert len(events) == 3
        assert events[0].code == EVENT_JOB_EXECUTED
        assert events[1].code == EVENT_JOB_EXECUTED
        assert events[2].code == EVENT_JOB_EXECUTED
        assert vals == [3]

    def test_print_jobs(self, scheduler):
        out = StringIO()
        scheduler.print_jobs(out=out)
        expected = 'Jobstore default:%s'\
                   '    No scheduled jobs%s' % (os.linesep, os.linesep)
        assert out.getvalue() == expected

        scheduler.add_job(copy, 'date', [datetime(2200, 5, 19)], args=[()])
        out = StringIO()
        scheduler.print_jobs(out=out)
        expected = 'Jobstore default:%s    '\
            'copy (trigger: date[2200-05-19 00:00:00 DUMMYTZ], '\
            'next run at: 2200-05-19 00:00:00 DUMMYTZ)%s' % (os.linesep, os.linesep)
        assert out.getvalue() == expected

    def test_jobstore(self, scheduler):
        scheduler.add_jobstore(MemoryJobStore(), 'dummy')
        job = scheduler.add_job(lambda: None, 'date', [datetime(2200, 7, 24)], jobstore='dummy')
        assert scheduler.get_jobs() == [job]
        scheduler.remove_jobstore('dummy')
        assert scheduler.get_jobs() == []

    def test_remove_nonexistent_jobstore(self, scheduler):
        """Tests that KeyError is raised when trying to remove a job store that doesn't exist."""

        pytest.raises(KeyError, scheduler.remove_jobstore, 'dummy2')

    def test_job_next_run_time(self, scheduler):
        """Tests against bug #5."""

        vals = [0]
        job = scheduler.add_job(increment, 'interval', {'seconds': 1, 'start_date': dummy_datetime},
                                args=(vals,), misfire_grace_time=3)

        scheduler.now = job.next_run_time
        scheduler._process_jobs()
        assert vals == [1]

        scheduler._process_jobs()
        assert vals == [1]

        scheduler.now = job.next_run_time + timedelta(seconds=1)
        scheduler._process_jobs()
        assert vals == [2]

    def test_max_instances(self, scheduler):
        """Tests that the maximum instance limit on a job is respected and that missed job events are dispatched when
        the job cannot be run due to the instance limitation.
        """

        def wait_event():
            vals[0] += 1
            event.wait(2)

        vals = [0]
        events = []
        event = Event()
        shutdown_event = Event()
        scheduler._threadpool = ThreadPool()
        scheduler.add_listener(events.append, EVENT_JOB_EXECUTED | EVENT_JOB_MISSED)
        scheduler.add_listener(lambda e: shutdown_event.set(), EVENT_SCHEDULER_SHUTDOWN)
        scheduler.add_job(wait_event, 'interval', {'seconds': 1, 'start_date': dummy_datetime}, max_instances=2,
                          max_runs=4)
        for _ in range(4):
            scheduler._process_jobs()
            scheduler.now += timedelta(seconds=1)
        event.set()
        scheduler.shutdown()
        shutdown_event.wait(2)

        assert vals == [2]
        assert len(events) == 4
        assert events[0].code == EVENT_JOB_MISSED
        assert events[1].code == EVENT_JOB_MISSED
        assert events[2].code == EVENT_JOB_EXECUTED
        assert events[3].code == EVENT_JOB_EXECUTED

    def test_scheduler_double_start(self, scheduler):
        pytest.raises(SchedulerAlreadyRunningError, scheduler.start)

    def test_scheduler_configure_running(self, scheduler):
        pytest.raises(SchedulerAlreadyRunningError, scheduler.configure, {})

    def test_scheduler_double_shutdown(self, scheduler):
        scheduler.shutdown()
        pytest.raises(SchedulerNotRunningError, scheduler.shutdown, False)


class SchedulerImplementationTestBase(object):
    @pytest.fixture
    def scheduler(self, request):
        sched = self.create_scheduler()
        request.addfinalizer(lambda: self.finish(sched))
        return sched

    def create_scheduler(self):
        raise NotImplementedError

    def create_event(self):
        return Event()

    def process_events(self):
        pass

    def finish(self, sched):
        pass

    def test_scheduler_implementation(self, scheduler):
        """Tests that starting the scheduler eventually calls _process_jobs()."""

        class TimeRoller(object):
            def __init__(self, start, step):
                self.now = start
                self.step = timedelta(seconds=step)

            def next(self):
                return self.now + self.step

            def __call__(self):
                now = self.now
                self.now = self.next()
                return now

        events = []
        vals = [0]
        job_removed_event = self.create_event()
        shutdown_event = self.create_event()
        scheduler._threadpool = DummyThreadPool()

        # Test that pending jobs are added (and if due, executed) when the scheduler starts
        scheduler._current_time = time_roller = TimeRoller(dummy_datetime, 0.2)
        scheduler.add_listener(events.append)
        scheduler.add_listener(lambda e: job_removed_event.set(), EVENT_JOBSTORE_JOB_REMOVED)
        scheduler.add_job(increment, 'date', [time_roller.next()], args=(vals,))
        scheduler.start()
        self.process_events()
        job_removed_event.wait(2)
        assert job_removed_event.is_set()
        assert vals[0] == 1
        assert len(events) == 5
        assert events[0].code == EVENT_JOBSTORE_ADDED
        assert events[1].code == EVENT_JOBSTORE_JOB_ADDED
        assert events[2].code == EVENT_SCHEDULER_START
        assert events[3].code == EVENT_JOB_EXECUTED
        assert events[4].code == EVENT_JOBSTORE_JOB_REMOVED
        del events[:]
        job_removed_event.clear()

        # Test that adding a job causes it to be executed after the specified delay
        job = scheduler.add_job(increment, 'date', [time_roller.next() + time_roller.step * 2], args=(vals,))
        self.process_events()
        sleep(0.5)
        self.process_events()
        job_removed_event.wait(2)
        assert job_removed_event.is_set()
        assert vals[0] == 2
        assert len(events) == 3
        assert events[0].code == EVENT_JOBSTORE_JOB_ADDED
        assert events[1].code == EVENT_JOB_EXECUTED
        assert events[2].code == EVENT_JOBSTORE_JOB_REMOVED
        del events[:]
        job_removed_event.clear()

        # Test that shutting down the scheduler emits the proper event
        scheduler.add_listener(lambda e: shutdown_event.set(), EVENT_SCHEDULER_SHUTDOWN)
        scheduler.shutdown()
        self.process_events()
        shutdown_event.wait(2)
        assert shutdown_event.is_set()
        assert len(events) == 1
        assert events[0].code == EVENT_SCHEDULER_SHUTDOWN


class TestBlockingScheduler(SchedulerImplementationTestBase):
    def create_scheduler(self):
        sched = BlockingScheduler()
        self.thread = Thread(target=sched.start)
        sched.start = self.thread.start
        return sched

    def finish(self, sched):
        self.thread.join()


class TestBackgroundScheduler(SchedulerImplementationTestBase):
    def create_scheduler(self):
        return BackgroundScheduler()


class TestAsyncIOScheduler(SchedulerImplementationTestBase):
    def create_scheduler(self):
        asyncio = pytest.importorskip('apscheduler.schedulers.asyncio')
        sched = asyncio.AsyncIOScheduler()
        self.thread = Thread(target=sched._eventloop.run_forever)
        self.thread.start()
        return sched

    def finish(self, sched):
        sched._eventloop.call_soon_threadsafe(sched._eventloop.stop)
        self.thread.join()


class TestGeventScheduler(SchedulerImplementationTestBase):
    def create_scheduler(self):
        gevent = pytest.importorskip('apscheduler.schedulers.gevent')
        return gevent.GeventScheduler()

    def create_event(self):
        from gevent.event import Event
        return Event()


class TestTornadoScheduler(SchedulerImplementationTestBase):
    def create_scheduler(self):
        tornado = pytest.importorskip('apscheduler.schedulers.tornado')
        sched = tornado.TornadoScheduler()
        self.thread = Thread(target=sched._ioloop.start)
        self.thread.start()
        return sched

    def finish(self, sched):
        sched._ioloop.add_callback(sched._ioloop.stop)
        self.thread.join()


class TestTwistedScheduler(SchedulerImplementationTestBase):
    def create_scheduler(self):
        twisted = pytest.importorskip('apscheduler.schedulers.twisted')
        sched = twisted.TwistedScheduler()
        self.thread = Thread(target=sched._reactor.run, args=(False,))
        self.thread.start()
        return sched

    def finish(self, sched):
        sched._reactor.callFromThread(sched._reactor.stop)
        self.thread.join()


class TestQtScheduler(SchedulerImplementationTestBase):
    def create_scheduler(self):
        qt = pytest.importorskip('apscheduler.schedulers.qt')
        from PySide.QtCore import QCoreApplication
        QCoreApplication([])
        return qt.QtScheduler()

    def process_events(self):
        from PySide.QtCore import QCoreApplication
        QCoreApplication.processEvents()
