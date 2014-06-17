import time

import pytest

from apscheduler.events import EVENT_JOB_ERROR
from apscheduler.executors.base import MaxInstancesReachedError


try:
    from unittest.mock import Mock, MagicMock
except ImportError:
    from mock import Mock, MagicMock


@pytest.fixture
def mock_scheduler():
    scheduler_ = Mock([])
    scheduler_._create_lock = MagicMock()
    return scheduler_


@pytest.fixture
def threadpoolexecutor(request):
    from apscheduler.executors.pool import ThreadPoolExecutor
    executor = ThreadPoolExecutor()
    request.addfinalizer(executor.shutdown)
    return executor


@pytest.fixture
def processpoolexecutor(request):
    from apscheduler.executors.pool import ProcessPoolExecutor
    executor = ProcessPoolExecutor()
    request.addfinalizer(executor.shutdown)
    return executor


@pytest.fixture(params=[threadpoolexecutor, processpoolexecutor], ids=['threadpool', 'processpool'])
def executor(request, mock_scheduler):
    executor_ = request.param(request)
    executor_.start(mock_scheduler, 'dummy')
    request.addfinalizer(executor_.shutdown)
    return executor_


def wait_event():
    time.sleep(0.2)
    return 'test'


def failure():
    raise Exception('test failure')


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


def test_job_error(mock_scheduler, executor, create_job, freeze_time):
    """Tests that a job error event is delivered to the scheduler if the job itself raises an exception."""

    mock_scheduler._dispatch_event = MagicMock()
    job = create_job(func=failure)
    executor.submit_job(job, [freeze_time.current])
    executor.shutdown()

    assert mock_scheduler._dispatch_event.call_count == 1
    event = mock_scheduler._dispatch_event.call_args[0][0]
    assert event.code == EVENT_JOB_ERROR
    assert str(event.exception) == 'test failure'
    assert isinstance(event.traceback, str)
