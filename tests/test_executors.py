import gc
import time
from asyncio import CancelledError
from datetime import datetime
from threading import Event
from types import TracebackType
from unittest.mock import Mock, MagicMock, patch

import pytest
from pytz import UTC, utc

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MISSED, EVENT_JOB_EXECUTED
from apscheduler.executors.asyncio import AsyncIOExecutor
from apscheduler.executors.base import MaxInstancesReachedError, run_job
from apscheduler.executors.tornado import TornadoExecutor
from apscheduler.job import Job
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.schedulers.base import BaseScheduler
from apscheduler.schedulers.tornado import TornadoScheduler


@pytest.fixture
def mock_scheduler(timezone):
    scheduler_ = Mock(BaseScheduler, timezone=timezone)
    scheduler_._create_lock = MagicMock()
    return scheduler_


@pytest.fixture(params=['threadpool', 'processpool'])
def executor(request, mock_scheduler):
    if request.param == 'threadpool':
        from apscheduler.executors.pool import ThreadPoolExecutor
        executor_ = ThreadPoolExecutor()
    else:
        from apscheduler.executors.pool import ProcessPoolExecutor
        executor_ = ProcessPoolExecutor()

    executor_.start(mock_scheduler, 'dummy')
    yield executor_
    executor_.shutdown()


def wait_event():
    time.sleep(0.2)
    return 'test'


def failure():
    raise Exception('test failure')


def success():
    return 5


def test_max_instances(mock_scheduler, executor, create_job, freeze_time):
    """Tests that the maximum instance limit on a job is respected."""
    events = []
    mock_scheduler._dispatch_event = lambda event: events.append(event)
    job = create_job(func=wait_event, max_instances=2, next_run_time=None)
    executor.submit_job(job, [freeze_time.current])
    executor.submit_job(job, [freeze_time.current])

    pytest.raises(MaxInstancesReachedError, executor.submit_job, job, [freeze_time.current])
    executor.shutdown()
    assert len(events) == 2
    assert events[0].retval == 'test'
    assert events[1].retval == 'test'


@pytest.mark.parametrize('event_code,func', [
    (EVENT_JOB_EXECUTED, success),
    (EVENT_JOB_MISSED, failure),
    (EVENT_JOB_ERROR, failure)
], ids=['executed', 'missed', 'error'])
def test_submit_job(mock_scheduler, executor, create_job, freeze_time, timezone, event_code, func):
    """
    Tests that an EVENT_JOB_EXECUTED event is delivered to the scheduler if the job was
    successfully executed.

    """
    mock_scheduler._dispatch_event = MagicMock()
    job = create_job(func=func, id='foo')
    job._jobstore_alias = 'test_jobstore'
    run_time = (timezone.localize(datetime(1970, 1, 1)) if event_code == EVENT_JOB_MISSED else
                freeze_time.current)
    executor.submit_job(job, [run_time])
    executor.shutdown()

    assert mock_scheduler._dispatch_event.call_count == 1
    event = mock_scheduler._dispatch_event.call_args[0][0]
    assert event.code == event_code
    assert event.job_id == 'foo'
    assert event.jobstore == 'test_jobstore'

    if event_code == EVENT_JOB_EXECUTED:
        assert event.retval == 5
    elif event_code == EVENT_JOB_ERROR:
        assert str(event.exception) == 'test failure'
        assert isinstance(event.traceback, str)


class FauxJob(object):
    id = 'abc'
    max_instances = 1
    _jobstore_alias = 'foo'


def dummy_run_job(job, jobstore_alias, run_times, logger_name):
    raise Exception('dummy')


def test_run_job_error(monkeypatch, executor):
    """Tests that _run_job_error is properly called if an exception is raised in run_job()"""
    def run_job_error(job_id, exc, traceback):
        assert job_id == 'abc'
        exc_traceback[:] = [exc, traceback]
        event.set()

    event = Event()
    exc_traceback = [None, None]
    monkeypatch.setattr('apscheduler.executors.base.run_job', dummy_run_job)
    monkeypatch.setattr('apscheduler.executors.pool.run_job', dummy_run_job)
    monkeypatch.setattr(executor, '_run_job_error', run_job_error)
    executor.submit_job(FauxJob(), [])

    event.wait(5)
    assert str(exc_traceback[0]) == "dummy"
    if exc_traceback[1] is not None:
        assert isinstance(exc_traceback[1], TracebackType)


def test_run_job_memory_leak():
    class FooBar(object):
        pass

    def func():
        foo = FooBar()  # noqa: F841
        raise Exception('dummy')

    fake_job = Mock(Job, func=func, args=(), kwargs={}, misfire_grace_time=1)
    with patch('logging.getLogger'):
        for _ in range(5):
            run_job(fake_job, 'foo', [datetime.now(UTC)], __name__)

    foos = [x for x in gc.get_objects() if type(x) is FooBar]
    assert len(foos) == 0


@pytest.fixture
def asyncio_scheduler(event_loop):
    scheduler = AsyncIOScheduler(event_loop=event_loop)
    scheduler.start(paused=True)
    yield scheduler
    scheduler.shutdown(False)


@pytest.fixture
def asyncio_executor(asyncio_scheduler):
    executor = AsyncIOExecutor()
    executor.start(asyncio_scheduler, 'default')
    yield executor
    executor.shutdown()


@pytest.fixture
def tornado_scheduler(io_loop):
    scheduler = TornadoScheduler(io_loop=io_loop)
    scheduler.start(paused=True)
    yield scheduler
    scheduler.shutdown(False)


@pytest.fixture
def tornado_executor(tornado_scheduler):
    executor = TornadoExecutor()
    executor.start(tornado_scheduler, 'default')
    yield executor
    executor.shutdown()


async def waiter(sleep, exception):
    await sleep(0.1)
    if exception:
        raise Exception('dummy error')
    else:
        return True


@pytest.mark.parametrize('exception', [False, True])
@pytest.mark.asyncio
async def test_run_coroutine_job(asyncio_scheduler, asyncio_executor, exception):
    from asyncio import Future, sleep

    future = Future()
    job = asyncio_scheduler.add_job(waiter, 'interval', seconds=1, args=[sleep, exception])
    asyncio_executor._run_job_success = lambda job_id, events: future.set_result(events)
    asyncio_executor._run_job_error = lambda job_id, exc, tb: future.set_exception(exc)
    asyncio_executor.submit_job(job, [datetime.now(utc)])
    events = await future
    assert len(events) == 1
    if exception:
        assert str(events[0].exception) == 'dummy error'
    else:
        assert events[0].retval is True


@pytest.mark.parametrize('exception', [False, True])
@pytest.mark.gen_test
async def test_run_coroutine_job_tornado(tornado_scheduler, tornado_executor, exception):
    from tornado.concurrent import Future
    from tornado.gen import sleep

    future = Future()
    job = tornado_scheduler.add_job(waiter, 'interval', seconds=1, args=[sleep, exception])
    tornado_executor._run_job_success = lambda job_id, events: future.set_result(events)
    tornado_executor._run_job_error = lambda job_id, exc, tb: future.set_exception(exc)
    tornado_executor.submit_job(job, [datetime.now(utc)])
    events = await future
    assert len(events) == 1
    if exception:
        assert str(events[0].exception) == 'dummy error'
    else:
        assert events[0].retval is True


@pytest.mark.asyncio
async def test_asyncio_executor_shutdown(asyncio_scheduler, asyncio_executor):
    """Test that the AsyncIO executor cancels its pending tasks on shutdown."""
    from asyncio import sleep

    job = asyncio_scheduler.add_job(waiter, 'interval', seconds=1, args=[sleep, None])
    asyncio_executor.submit_job(job, [datetime.now(utc)])
    futures = asyncio_executor._pending_futures.copy()
    assert len(futures) == 1

    asyncio_executor.shutdown()
    with pytest.raises(CancelledError):
        await futures.pop()
