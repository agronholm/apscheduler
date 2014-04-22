from datetime import datetime, timedelta
from logging import StreamHandler, ERROR, getLogger
from threading import Thread
from copy import copy
import os

import pytest

from apscheduler.executors.pool import PoolExecutor
from apscheduler.jobstores.base import BaseJobStore
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.schedulers import SchedulerAlreadyRunningError, SchedulerNotRunningError
from apscheduler.schedulers.base import BaseScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.events import (SchedulerEvent, EVENT_JOB_EXECUTED, EVENT_SCHEDULER_START, EVENT_SCHEDULER_SHUTDOWN,
                                EVENT_JOB_MISSED, EVENT_JOBSTORE_ADDED, EVENT_JOBSTORE_JOB_ADDED,
                                EVENT_JOBSTORE_JOB_REMOVED, EVENT_JOBSTORE_JOB_MODIFIED)

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

try:
    from unittest.mock import MagicMock
except ImportError:
    from mock import MagicMock


class DummyException(Exception):
    pass


class DummyScheduler(BaseScheduler):
    def __init__(self, timezone, gconfig={}, **options):
        super(DummyScheduler, self).__init__(gconfig, timezone=timezone, **options)
        self.add_executor(PoolExecutor('debug'), 'default')

    def start(self):
        super(DummyScheduler, self).start()

    def shutdown(self, wait=True):
        super(DummyScheduler, self).shutdown()

    def _wakeup(self):
        self._process_jobs()


def increment(vals):
    vals[0] += 1


@pytest.fixture
def scheduler(request, freeze_time, timezone):
    sched = DummyScheduler(timezone)
    if 'start_scheduler' in request.keywords:
        sched.start()
        request.addfinalizer(lambda: sched.shutdown() if sched.running else None)

    return sched


@pytest.fixture
def logstream(request, scheduler):
    stream = StringIO()
    loghandler = StreamHandler(stream)
    loghandler.setLevel(ERROR)
    logger = getLogger('apscheduler')
    logger.addHandler(loghandler)
    request.addfinalizer(lambda: logger.removeHandler(loghandler))
    return stream


class TestOfflineScheduler(object):
    def test_jobstore_twice(self, scheduler):
        with pytest.raises(KeyError):
            scheduler.add_jobstore(MemoryJobStore(), 'dummy')
            scheduler.add_jobstore(MemoryJobStore(), 'dummy')

    def test_add_job_by_reference(self, scheduler):
        job = scheduler.add_job('copy:copy', 'date', run_date=datetime(2200, 7, 24), args=[()])
        assert job.func == 'copy:copy'

    def test_modify_job(self, scheduler):
        scheduler.add_job(lambda: None, 'interval', seconds=1, id='foo')
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
        assert scheduler._job_defaults['misfire_grace_time'] == 1
        assert scheduler._job_defaults['coalesce'] is True

    def test_configure_prefix(self, scheduler):
        global_options = {
            'apscheduler.job_defaults.misfire_grace_time': 2,
            'apscheduler.job_defaults.coalesce': False
        }
        scheduler.configure(global_options)
        assert scheduler._job_defaults['misfire_grace_time'] == 2
        assert scheduler._job_defaults['coalesce'] is False

    def test_add_listener(self, scheduler):
        val = []
        scheduler.add_listener(val.append)

        event = SchedulerEvent(EVENT_SCHEDULER_START)
        scheduler._dispatch_event(event)
        assert len(val) == 1
        assert val[0] == event

        event = SchedulerEvent(EVENT_SCHEDULER_SHUTDOWN)
        scheduler._dispatch_event(event)
        assert len(val) == 2
        assert val[1] == event

        scheduler.remove_listener(val.append)
        scheduler._dispatch_event(event)
        assert len(val) == 2

    def test_pending_jobs(self, scheduler):
        """Tests that pending jobs are properly added to the jobs list, but only when the scheduler is started."""

        job = scheduler.add_job(lambda: None, 'date', run_date=datetime(9999, 9, 9))
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

        scheduler.add_job(copy, 'date', run_date=datetime(2200, 5, 19), args=[()])
        out = StringIO()
        scheduler.print_jobs(out=out)
        expected = 'Pending jobs:%s    '\
            'copy (trigger: date[2200-05-19 00:00:00 CET], '\
            'next run at: None)%s' % (os.linesep, os.linesep)
        assert out.getvalue() == expected


@pytest.mark.start_scheduler
class TestRunningScheduler(object):
    def test_add_job_object(self, scheduler, freeze_time):
        """Tests that any callable object is accepted (and not just functions)."""

        class A(object):
            def __init__(self):
                self.val = 0

            def __call__(self):
                self.val += 1

        a = A()
        job = scheduler.add_job(a, 'interval', seconds=1)
        freeze_time.set(job.next_run_time)
        scheduler._process_jobs()
        assert a.val == 1

    def test_add_job_method(self, scheduler, freeze_time):
        """Tests that bound methods can be scheduled (at least with MemoryJobStore)."""

        class A(object):
            def __init__(self):
                self.val = 0

            def method(self):
                self.val += 1

        a = A()
        job = scheduler.add_job(a.method, 'interval', seconds=1)
        freeze_time.set(job.next_run_time)
        scheduler._process_jobs()
        assert a.val == 1

    def test_add_job_no_trigger(self, scheduler):
        """Tests that adding a job without a trigger causes it to be executed immediately."""

        vals = [0]
        scheduler.add_job(increment, args=(vals,))
        assert vals == [1]

    def test_add_job_decorator(self, scheduler):
        """Tests that the scheduled_job decorator works."""

        @scheduler.scheduled_job('interval', seconds=1)
        def dummyjob():
            pass

        jobs = scheduler.get_jobs()
        assert len(jobs) == 1
        assert jobs[0].name == 'dummyjob'

    def replace_job(self, scheduler, create_job):
        """Tests that an existing job can be replaced if the replace_existing flag is True."""

        job = create_job(lambda: None, id='foo', runs=3)
        scheduler.add_job(job)

        @scheduler.scheduled_job('interval', id='foo', seconds=1)
        def dummyjob():
            pass

        job = scheduler.get_job('foo')
        assert job.func is dummyjob
        assert job.runs == 3

    def test_modify_job(self, scheduler):
        events = []
        scheduler.add_listener(events.append, EVENT_JOBSTORE_JOB_MODIFIED)
        scheduler.add_job(lambda: None, 'interval', seconds=1, id='foo')
        scheduler.modify_job('foo', max_runs=3456)
        assert len(events) == 1

        job = scheduler.get_jobs()[0]
        assert job.max_runs == 3456

    def test_modify_job_next_run_time(self, scheduler):
        """Tests that modifying a job's next_run_time will cause the scheduler to be woken up."""

        scheduler.add_job(lambda: None, 'interval', seconds=1, id='foo')
        scheduler._wakeup = MagicMock()
        scheduler.modify_job('foo', next_run_time=datetime(2014, 4, 1))
        scheduler._wakeup.assert_called_once_with()

    def test_pause_job(self, scheduler, freeze_time):
        """Tests that pausing a job causes the scheduler to have the job store update the job with no next run time."""

        jobstore = MagicMock(BaseJobStore)
        scheduler._wakeup = MagicMock()
        scheduler.add_jobstore(jobstore, 'mock')
        scheduler.add_job(lambda: None, 'interval', seconds=10, id='foo', jobstore='mock')
        job = jobstore.add_job.call_args[0][0]
        assert job.next_run_time == freeze_time.current + timedelta(seconds=10)

        jobstore.lookup_job = MagicMock(return_value=job)
        scheduler.pause_job('foo', 'mock')
        jobstore.lookup_job.assert_called_once_with('foo')
        assert jobstore.update_job.call_count == 1
        job = jobstore.update_job.call_args[0][0]
        assert job.next_run_time is None
        assert scheduler._wakeup.call_count == 3

    def test_resume_job(self, scheduler, create_job):
        """Tests that resuming a job causes the scheduler to have the job store update the job with a next run time."""

        job = create_job(func=lambda: None)
        assert job.next_run_time is None
        jobstore = MagicMock(BaseJobStore, lookup_job=lambda job_id: job)
        scheduler._wakeup = MagicMock()
        scheduler.add_jobstore(jobstore, 'mock')

        scheduler.resume_job(job.id, 'mock')
        jobstore.update_job.assert_called_once_with(job)
        assert job.next_run_time == job.trigger.run_date
        assert scheduler._wakeup.call_count == 2

    def test_resume_job_remove(self, scheduler, create_job, timezone):
        """
        Tests that resuming a job causes the scheduler to remove the job when a next run time cannot be calculated for
        it.
        """

        job = create_job(func=lambda: None, trigger_args={'run_date': datetime(2000, 1, 1), 'timezone': timezone})
        assert job.next_run_time is None
        jobstore = MagicMock(BaseJobStore, lookup_job=lambda job_id: job)
        scheduler._wakeup = MagicMock()
        scheduler.add_jobstore(jobstore, 'mock')

        scheduler.resume_job(job.id, 'mock')
        jobstore.remove_job.assert_called_once_with(job.id)
        assert scheduler._wakeup.call_count == 1

    def test_remove_job(self, scheduler):
        vals = [0]
        events = []
        scheduler.add_listener(events.append, EVENT_JOBSTORE_JOB_REMOVED)
        job = scheduler.add_job(increment, 'cron', args=(vals,))
        scheduler.now = job.next_run_time
        scheduler._process_jobs()
        assert vals[0] == 1

        scheduler.remove_job(job.id)
        assert len(events) == 1
        scheduler._process_jobs()
        assert vals[0] == 1

    def test_remove_all_jobs(self, scheduler, freeze_time):
        """Tests that removing all jobs clears all job stores."""

        vals = [0]
        scheduler.add_jobstore(MemoryJobStore(), 'alter')
        scheduler.add_job(increment, 'interval', seconds=1, args=(vals,))
        scheduler.add_job(increment, 'interval', seconds=1, args=(vals,), jobstore='alter')
        scheduler.remove_all_jobs()
        assert scheduler.get_jobs() == []

    def test_remove_all_jobs_specific_jobstore(self, scheduler):
        """Tests that removing all jobs from a specific job store does not affect the rest."""

        vals = [0]
        scheduler.add_jobstore(MemoryJobStore(), 'alter')
        scheduler.add_job(increment, 'interval', seconds=1, args=(vals,))
        job2 = scheduler.add_job(increment, 'interval', seconds=1, args=(vals,), jobstore='alter')
        scheduler.remove_all_jobs('default')
        assert scheduler.get_jobs() == [job2]

    def test_job_finished(self, scheduler, freeze_time):
        vals = [0]
        job = scheduler.add_job(increment, 'interval', args=(vals,), max_runs=1)
        freeze_time.set(job.next_run_time)
        scheduler._process_jobs()
        assert vals == [1]
        assert job not in scheduler.get_jobs()

    def test_job_exception(self, scheduler, freeze_time, logstream):
        def failure():
            raise DummyException

        job = scheduler.add_job(failure, 'date', run_date=datetime(9999, 9, 9))
        freeze_time.set(job.next_run_time)
        scheduler._process_jobs()
        assert 'DummyException' in logstream.getvalue()

    def test_misfire_grace_time(self, scheduler):
        scheduler._job_defaults['misfire_grace_time'] = 3
        job = scheduler.add_job(lambda: None, 'interval', seconds=1)
        assert job.misfire_grace_time == 3

        job = scheduler.add_job(lambda: None, 'interval', seconds=1, misfire_grace_time=2)
        assert job.misfire_grace_time == 2

    def test_coalesce_on(self, scheduler, freeze_time):
        """Tests that the job is only executed once when it is scheduled to be executed twice in a row."""

        vals = [0]
        events = []
        scheduler.add_listener(events.append, EVENT_JOB_EXECUTED | EVENT_JOB_MISSED)
        scheduler._wakeup = lambda: None
        job = scheduler.add_job(increment, 'interval', seconds=1, start_date=freeze_time.current, args=(vals,),
                                coalesce=True, misfire_grace_time=2)

        # Turn the clock 2 seconds forward
        freeze_time.set(freeze_time.current + timedelta(seconds=2))

        scheduler._process_jobs()
        job.refresh()
        assert job.runs == 1
        assert len(events) == 1
        assert events[0].code == EVENT_JOB_EXECUTED
        assert vals == [1]

    def test_coalesce_off(self, scheduler, freeze_time):
        """Tests that every scheduled run for the job is executed even when they are in the past
        (but still within misfire_grace_time).
        """

        vals = [0]
        events = []
        scheduler.add_listener(events.append, EVENT_JOB_EXECUTED | EVENT_JOB_MISSED)
        job = scheduler.add_job(increment, 'interval', seconds=1, start_date=freeze_time.current, args=(vals,),
                                coalesce=False, misfire_grace_time=2)

        # Turn the clock 2 seconds forward
        freeze_time.set(freeze_time.current + timedelta(seconds=2))

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

        scheduler.add_job(copy, 'date', run_date=datetime(2200, 5, 19), args=[()])
        out = StringIO()
        scheduler.print_jobs(out=out)
        expected = 'Jobstore default:%s    '\
            'copy (trigger: date[2200-05-19 00:00:00 CET], '\
            'next run at: 2200-05-19 00:00:00 CET)%s' % (os.linesep, os.linesep)
        assert out.getvalue() == expected

    def test_jobstore(self, scheduler):
        scheduler.add_jobstore(MemoryJobStore(), 'dummy')
        job = scheduler.add_job(lambda: None, 'date', run_date=datetime(2200, 7, 24), jobstore='dummy')
        assert scheduler.get_jobs() == [job]
        scheduler.remove_jobstore('dummy')
        assert scheduler.get_jobs() == []

    def test_remove_nonexistent_jobstore(self, scheduler):
        """Tests that KeyError is raised when trying to remove a job store that doesn't exist."""

        pytest.raises(KeyError, scheduler.remove_jobstore, 'dummy2')

    def test_job_next_run_time(self, scheduler, freeze_time):
        """Tests against bug #5."""

        vals = [0]
        scheduler._wakeup = lambda: None
        job = scheduler.add_job(increment, 'interval', seconds=1, start_date=freeze_time.current, args=(vals,),
                                misfire_grace_time=3)

        freeze_time.set(job.next_run_time)
        scheduler._process_jobs()
        assert vals == [1]

        scheduler._process_jobs()
        assert vals == [1]

        freeze_time.set(job.next_run_time + timedelta(seconds=1))
        scheduler._process_jobs()
        assert vals == [2]

    def test_scheduler_double_start(self, scheduler):
        pytest.raises(SchedulerAlreadyRunningError, scheduler.start)

    def test_scheduler_configure_running(self, scheduler):
        pytest.raises(SchedulerAlreadyRunningError, scheduler.configure, {})

    def test_scheduler_double_shutdown(self, scheduler):
        scheduler.shutdown()
        pytest.raises(SchedulerNotRunningError, scheduler.shutdown, False)


class SchedulerImplementationTestBase(object):
    @pytest.fixture(autouse=True)
    def executor(self, scheduler):
        scheduler.add_executor(PoolExecutor('debug'))

    @pytest.fixture
    def start_scheduler(self, request, scheduler):
        def cleanup():
            if scheduler.running:
                scheduler.shutdown()

        request.addfinalizer(cleanup)
        return scheduler.start

    @pytest.fixture
    def eventqueue(self, scheduler):
        from six.moves.queue import Queue
        events = Queue()
        scheduler.add_listener(events.put)
        return events

    def wait_event(self, queue):
        return queue.get(True, 1)

    def test_add_pending_job(self, scheduler, freeze_time, eventqueue, start_scheduler):
        """Tests that pending jobs are added (and if due, executed) when the scheduler starts."""

        freeze_time.set_increment(timedelta(seconds=0.2))
        scheduler.add_job(lambda x, y: x + y, 'date', args=[1, 2], run_date=freeze_time.next())
        start_scheduler()

        assert self.wait_event(eventqueue).code == EVENT_JOBSTORE_ADDED
        assert self.wait_event(eventqueue).code == EVENT_JOBSTORE_JOB_ADDED
        assert self.wait_event(eventqueue).code == EVENT_SCHEDULER_START
        event = self.wait_event(eventqueue)
        assert event.code == EVENT_JOB_EXECUTED
        assert event.retval == 3
        assert self.wait_event(eventqueue).code == EVENT_JOBSTORE_JOB_REMOVED

    def test_add_live_job(self, scheduler, freeze_time, eventqueue, start_scheduler):
        """Tests that adding a job causes it to be executed after the specified delay."""

        freeze_time.set_increment(timedelta(seconds=0.2))
        start_scheduler()
        assert self.wait_event(eventqueue).code == EVENT_JOBSTORE_ADDED
        assert self.wait_event(eventqueue).code == EVENT_SCHEDULER_START

        scheduler.add_job(lambda x, y: x + y, 'date', args=[1, 2],
                          run_date=freeze_time.next() + freeze_time.increment * 2)
        assert self.wait_event(eventqueue).code == EVENT_JOBSTORE_JOB_ADDED
        event = self.wait_event(eventqueue)
        assert event.code == EVENT_JOB_EXECUTED
        assert event.retval == 3
        assert self.wait_event(eventqueue).code == EVENT_JOBSTORE_JOB_REMOVED

    def test_shutdown(self, scheduler, eventqueue, start_scheduler):
        """Tests that shutting down the scheduler emits the proper event."""

        start_scheduler()
        assert self.wait_event(eventqueue).code == EVENT_JOBSTORE_ADDED
        assert self.wait_event(eventqueue).code == EVENT_SCHEDULER_START

        scheduler.shutdown()
        assert self.wait_event(eventqueue).code == EVENT_SCHEDULER_SHUTDOWN


class TestBlockingScheduler(SchedulerImplementationTestBase):
    @pytest.fixture
    def scheduler(self):
        return BlockingScheduler()

    @pytest.fixture
    def start_scheduler(self, request, scheduler):
        def cleanup():
            if scheduler.running:
                scheduler.shutdown()
            thread.join()

        request.addfinalizer(cleanup)
        thread = Thread(target=scheduler.start)
        return thread.start


class TestBackgroundScheduler(SchedulerImplementationTestBase):
    @pytest.fixture
    def scheduler(self):
        return BackgroundScheduler()


class TestAsyncIOScheduler(SchedulerImplementationTestBase):
    @pytest.fixture
    def event_loop(self):
        asyncio = pytest.importorskip('asyncio')
        return asyncio.new_event_loop()

    @pytest.fixture
    def scheduler(self, event_loop):
        asyncio = pytest.importorskip('apscheduler.schedulers.asyncio')
        return asyncio.AsyncIOScheduler(event_loop=event_loop)

    @pytest.fixture
    def start_scheduler(self, request, event_loop, scheduler):
        def cleanup():
            if scheduler.running:
                event_loop.call_soon_threadsafe(scheduler.shutdown)
            event_loop.call_soon_threadsafe(event_loop.stop)
            thread.join()

        event_loop.call_soon_threadsafe(scheduler.start)
        request.addfinalizer(cleanup)
        thread = Thread(target=event_loop.run_forever)
        return thread.start


class TestGeventScheduler(SchedulerImplementationTestBase):
    @pytest.fixture
    def scheduler(self):
        gevent = pytest.importorskip('apscheduler.schedulers.gevent')
        return gevent.GeventScheduler()

    @pytest.fixture
    def calc_event(self):
        from gevent.event import Event
        return Event()

    @pytest.fixture
    def eventqueue(self, scheduler):
        from gevent.queue import Queue
        events = Queue()
        scheduler.add_listener(events.put)
        return events


class TestTornadoScheduler(SchedulerImplementationTestBase):
    @pytest.fixture
    def io_loop(self):
        ioloop = pytest.importorskip('tornado.ioloop')
        return ioloop.IOLoop()

    @pytest.fixture
    def scheduler(self, io_loop):
        tornado = pytest.importorskip('apscheduler.schedulers.tornado')
        return tornado.TornadoScheduler(io_loop=io_loop)

    @pytest.fixture
    def start_scheduler(self, request, io_loop, scheduler):
        def cleanup():
            if scheduler.running:
                io_loop.add_callback(scheduler.shutdown)
            io_loop.add_callback(io_loop.stop)
            thread.join()

        io_loop.add_callback(scheduler.start)
        request.addfinalizer(cleanup)
        thread = Thread(target=io_loop.start)
        return thread.start


class TestTwistedScheduler(SchedulerImplementationTestBase):
    @pytest.fixture
    def reactor(self):
        selectreactor = pytest.importorskip('twisted.internet.selectreactor')
        return selectreactor.SelectReactor()

    @pytest.fixture
    def scheduler(self, reactor):
        twisted = pytest.importorskip('apscheduler.schedulers.twisted')
        return twisted.TwistedScheduler(reactor=reactor)

    @pytest.fixture
    def start_scheduler(self, request, reactor, scheduler):
        def cleanup():
            if scheduler.running:
                reactor.callFromThread(scheduler.shutdown)
            reactor.callFromThread(reactor.stop)
            thread.join()

        reactor.callFromThread(scheduler.start)
        request.addfinalizer(cleanup)
        thread = Thread(target=reactor.run, args=(False,))
        return thread.start


@pytest.mark.skip
class TestQtScheduler(SchedulerImplementationTestBase):
    @pytest.fixture(scope='class')
    def coreapp(self):
        QtCore = pytest.importorskip('PySide.QtCore')
        QtCore.QCoreApplication([])

    @pytest.fixture
    def scheduler(self, coreapp):
        qt = pytest.importorskip('apscheduler.schedulers.qt')
        return qt.QtScheduler()

    def wait_event(self, queue):
        from PySide.QtCore import QCoreApplication

        while queue.empty():
            QCoreApplication.processEvents()
        return queue.get_nowait()
