from datetime import datetime

import pytest
from anyio import fail_after, sleep
from apscheduler.abc import Job
from apscheduler.events import JobAdded, JobDeadlineMissed, JobFailed, JobSuccessful, JobUpdated
from apscheduler.workers.local import LocalExecutor

pytestmark = pytest.mark.anyio


@pytest.mark.parametrize('sync', [True, False], ids=['sync', 'async'])
@pytest.mark.parametrize('fail', [False, True], ids=['success', 'fail'])
async def test_run_job_nonscheduled_success(sync, fail):
    def sync_func(*args, **kwargs):
        nonlocal received_args, received_kwargs
        received_args = args
        received_kwargs = kwargs
        if fail:
            raise Exception('failing as requested')
        else:
            return 'success'

    async def async_func(*args, **kwargs):
        nonlocal received_args, received_kwargs
        received_args = args
        received_kwargs = kwargs
        if fail:
            raise Exception('failing as requested')
        else:
            return 'success'

    received_args = received_kwargs = None
    events = []
    async with LocalExecutor() as worker:
        await worker.subscribe(events.append)

        job = Job('task_id', sync_func if sync else async_func, args=(1, 2), kwargs={'x': 'foo'})
        await worker.submit_job(job)

    async with fail_after(1):
        while len(events) < 3:
            await sleep(0)

    assert received_args == (1, 2)
    assert received_kwargs == {'x': 'foo'}

    assert isinstance(events[0], JobAdded)
    assert events[0].job_id == job.id
    assert events[0].task_id == 'task_id'
    assert events[0].schedule_id is None
    assert events[0].scheduled_start_time is None

    assert isinstance(events[1], JobUpdated)
    assert events[1].job_id == job.id
    assert events[1].task_id == 'task_id'
    assert events[1].schedule_id is None
    assert events[1].scheduled_start_time is None

    assert events[2].job_id == job.id
    assert events[2].task_id == 'task_id'
    assert events[2].schedule_id is None
    assert events[2].scheduled_start_time is None
    if fail:
        assert isinstance(events[2], JobFailed)
        assert type(events[2].exception) is Exception
        assert isinstance(events[2].formatted_traceback, str)
    else:
        assert isinstance(events[2], JobSuccessful)
        assert events[2].return_value == 'success'


async def test_run_deadline_missed():
    def func():
        pytest.fail('This function should never be run')

    scheduled_start_time = datetime(2020, 9, 14)
    events = []
    async with LocalExecutor() as worker:
        await worker.subscribe(events.append)

        job = Job('task_id', func, args=(), kwargs={}, schedule_id='foo',
                  scheduled_start_time=scheduled_start_time,
                  start_deadline=datetime(2020, 9, 14, 1))
        await worker.submit_job(job)

    async with fail_after(1):
        while len(events) < 2:
            await sleep(0)

    assert isinstance(events[0], JobAdded)
    assert events[0].job_id == job.id
    assert events[0].task_id == 'task_id'
    assert events[0].schedule_id == 'foo'
    assert events[0].scheduled_start_time == scheduled_start_time

    assert isinstance(events[1], JobDeadlineMissed)
    assert events[1].job_id == job.id
    assert events[1].task_id == 'task_id'
    assert events[1].schedule_id == 'foo'
    assert events[1].scheduled_start_time == scheduled_start_time
