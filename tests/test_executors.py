from datetime import datetime
from threading import Event
from types import TracebackType
import os
import sys
import time

import pytest

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MISSED, EVENT_JOB_EXECUTED
from apscheduler.executors.base import MaxInstancesReachedError
from apscheduler.schedulers.base import BaseScheduler

try:
    from unittest.mock import Mock, MagicMock
except ImportError:
    from mock import Mock, MagicMock


@pytest.fixture
def mock_scheduler(timezone):
    scheduler_ = Mock(BaseScheduler, timezone=timezone)
    scheduler_._create_lock = MagicMock()
    return scheduler_


@pytest.yield_fixture(params=['threadpool', 'processpool'])
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

    event.wait(2)
    assert str(exc_traceback[0]) == "dummy"
    if exc_traceback[1] is not None:
        assert isinstance(exc_traceback[1], TracebackType)


_SIZE = 100 * (10 ** 6)  # ~ 100Mo


def _memleak_job():
    a = os.urandom(_SIZE)
    a.fail()


def _calculate_vms():
    import psutil
    process = psutil.Process(os.getpid())
    all_processes = [process] + list(process.children())
    return sum(p.memory_info().vms for p in all_processes)


@pytest.mark.skipif(hasattr(sys, 'pypy_version_info'), reason="""\
It's hard to predict when GC will happen on pypy.
See http://doc.pypy.org/en/release-2.4.x/garbage_collection.html
""")
def test_submit_job_memleak(monkeypatch, mock_scheduler, executor, create_job, freeze_time):
    """
    Test that executor does not suffer from memory leak when dealing with traceback objects.
    See https://github.com/agronholm/apscheduler/issues/235
    """
    import _pytest.logging

    pytest.importorskip('psutil')

    # Patch LogCaptureHandler because it keeps record of traceback objects, preventing GC.
    monkeypatch.setattr(_pytest.logging.LogCaptureHandler, 'emit', lambda *a, **k: None)

    mock_scheduler._dispatch_event = MagicMock()
    run_time = (freeze_time.current)
    safe_job = create_job(func=success, max_instances=10, id='foo')
    memleak_job = create_job(func=_memleak_job, max_instances=10, id='bar')
    safe_job._jobstore_alias = 'test_jobstore'
    memleak_job._jobstore_alias = 'test_jobstore'

    # Spawn as many workers as possible to prevent sudden RAM consumption
    for _ in range(10):
        executor.submit_job(safe_job, [run_time])

    # Calculate memory consumption before tasks
    before = _calculate_vms()

    for _ in range(10):
        executor.submit_job(memleak_job, [run_time])

    executor.shutdown()

    after = _calculate_vms()
    assert (after - before) < _SIZE
