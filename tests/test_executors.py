import time

import pytest

from apscheduler.executors.base import MaxInstancesReachedError
from apscheduler.executors.pool import PoolExecutor


try:
    from unittest.mock import Mock, MagicMock
except ImportError:
    from mock import Mock, MagicMock


@pytest.fixture
def scheduler():
    scheduler_ = Mock([])
    scheduler_._create_lock = MagicMock()
    return scheduler_


@pytest.fixture(params=['thread', 'process'])
def executor(request, scheduler):
    executor_ = PoolExecutor(request.param)
    executor_.start(scheduler, 'dummy')
    request.addfinalizer(executor_.shutdown)
    return executor_


def wait_event():
    time.sleep(0.2)
    return 'test'


def test_max_instances(scheduler, executor, create_job, freeze_time):
    """Tests that the maximum instance limit on a job is respected."""

    events = []
    scheduler._notify_listeners = lambda event: events.append(event)
    job = create_job(func=wait_event, max_instances=2, max_runs=3)
    executor.submit_job(job, [freeze_time.current])
    executor.submit_job(job, [freeze_time.current])

    pytest.raises(MaxInstancesReachedError, executor.submit_job, job, [freeze_time.current])
    executor.shutdown()
    assert len(events) == 2
    assert events[0].retval == 'test'
    assert events[1].retval == 'test'
