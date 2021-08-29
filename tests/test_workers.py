import threading
from datetime import datetime, timezone
from typing import Callable, List

import anyio
import pytest
from anyio import fail_after

from apscheduler.abc import Job
from apscheduler.datastores.sync.memory import MemoryDataStore
from apscheduler.events import (
    Event, JobAdded, JobCompleted, JobDeadlineMissed, JobFailed, JobStarted, WorkerStarted,
    WorkerStopped)
from apscheduler.workers.async_ import AsyncWorker
from apscheduler.workers.sync import Worker

pytestmark = pytest.mark.anyio


def sync_func(*args, fail: bool, **kwargs):
    if fail:
        raise Exception('failing as requested')
    else:
        return args, kwargs


async def async_func(*args, fail: bool, **kwargs):
    if fail:
        raise Exception('failing as requested')
    else:
        return args, kwargs


def fail_func():
    pytest.fail('This function should never be run')


class TestAsyncWorker:
    @pytest.mark.parametrize('target_func', [sync_func, async_func], ids=['sync', 'async'])
    @pytest.mark.parametrize('fail', [False, True], ids=['success', 'fail'])
    async def test_run_job_nonscheduled_success(self, target_func: Callable, fail: bool) -> None:
        def listener(received_event: Event):
            received_events.append(received_event)
            if len(received_events) == 4:
                event.set()

        received_events: List[Event] = []
        event = anyio.Event()
        data_store = MemoryDataStore()
        worker = AsyncWorker(data_store)
        worker.subscribe(listener)
        async with worker:
            job = Job('task_id', func=target_func, args=(1, 2), kwargs={'x': 'foo', 'fail': fail})
            await worker.data_store.add_job(job)
            with fail_after(3):
                await event.wait()

        # The worker was first started
        received_event = received_events.pop(0)
        assert isinstance(received_event, WorkerStarted)

        # Then a job was added
        received_event = received_events.pop(0)
        assert isinstance(received_event, JobAdded)
        assert received_event.job_id == job.id
        assert received_event.task_id == 'task_id'
        assert received_event.schedule_id is None

        # Then the job was started
        received_event = received_events.pop(0)
        assert isinstance(received_event, JobStarted)
        assert received_event.job_id == job.id
        assert received_event.task_id == 'task_id'
        assert received_event.schedule_id is None

        received_event = received_events.pop(0)
        if fail:
            # Then the job failed
            assert isinstance(received_event, JobFailed)
            assert isinstance(received_event.exception, str)
            assert isinstance(received_event.traceback, str)
        else:
            # Then the job finished successfully
            assert isinstance(received_event, JobCompleted)
            assert received_event.return_value == ((1, 2), {'x': 'foo'})

        # Finally, the worker was stopped
        received_event = received_events.pop(0)
        assert isinstance(received_event, WorkerStopped)

        # There should be no more events on the list
        assert not received_events

    async def test_run_deadline_missed(self) -> None:
        def listener(received_event: Event):
            received_events.append(received_event)
            if len(received_events) == 3:
                event.set()

        scheduled_start_time = datetime(2020, 9, 14, tzinfo=timezone.utc)
        received_events: List[Event] = []
        event = anyio.Event()
        data_store = MemoryDataStore()
        worker = AsyncWorker(data_store)
        worker.subscribe(listener)
        async with worker:
            job = Job('task_id', fail_func, args=(), kwargs={}, schedule_id='foo',
                      scheduled_fire_time=scheduled_start_time,
                      start_deadline=datetime(2020, 9, 14, 1, tzinfo=timezone.utc))
            await worker.data_store.add_job(job)
            with fail_after(3):
                await event.wait()

        # The worker was first started
        received_event = received_events.pop(0)
        assert isinstance(received_event, WorkerStarted)

        # Then a job was added
        received_event = received_events.pop(0)
        assert isinstance(received_event, JobAdded)
        assert received_event.job_id == job.id
        assert received_event.task_id == 'task_id'
        assert received_event.schedule_id == 'foo'

        # Then the deadline was missed
        received_event = received_events.pop(0)
        assert isinstance(received_event, JobDeadlineMissed)
        assert received_event.job_id == job.id
        assert received_event.task_id == 'task_id'
        assert received_event.schedule_id == 'foo'

        # Finally, the worker was stopped
        received_event = received_events.pop(0)
        assert isinstance(received_event, WorkerStopped)

        # There should be no more events on the list
        assert not received_events


class TestSyncWorker:
    @pytest.mark.parametrize('fail', [False, True], ids=['success', 'fail'])
    def test_run_job_nonscheduled(self, fail: bool) -> None:
        def listener(received_event: Event):
            received_events.append(received_event)
            if len(received_events) == 4:
                event.set()

        received_events: List[Event] = []
        event = threading.Event()
        data_store = MemoryDataStore()
        worker = Worker(data_store)
        worker.subscribe(listener)
        with worker:
            job = Job('task_id', func=sync_func, args=(1, 2), kwargs={'x': 'foo', 'fail': fail})
            worker.data_store.add_job(job)
            event.wait(5)

        # The worker was first started
        received_event = received_events.pop(0)
        assert isinstance(received_event, WorkerStarted)

        # Then a job was added
        received_event = received_events.pop(0)
        assert isinstance(received_event, JobAdded)
        assert received_event.job_id == job.id
        assert received_event.task_id == 'task_id'
        assert received_event.schedule_id is None

        # Then the job was started
        received_event = received_events.pop(0)
        assert isinstance(received_event, JobStarted)
        assert received_event.job_id == job.id
        assert received_event.task_id == 'task_id'
        assert received_event.schedule_id is None

        received_event = received_events.pop(0)
        if fail:
            # Then the job failed
            assert isinstance(received_event, JobFailed)
            assert isinstance(received_event.exception, str)
            assert isinstance(received_event.traceback, str)
        else:
            # Then the job finished successfully
            assert isinstance(received_event, JobCompleted)
            assert received_event.return_value == ((1, 2), {'x': 'foo'})

        # Finally, the worker was stopped
        received_event = received_events.pop(0)
        assert isinstance(received_event, WorkerStopped)

        # There should be no more events on the list
        assert not received_events

    def test_run_deadline_missed(self) -> None:
        def listener(worker_event: Event):
            received_events.append(worker_event)
            if len(received_events) == 3:
                event.set()

        scheduled_start_time = datetime(2020, 9, 14, tzinfo=timezone.utc)
        received_events: List[Event] = []
        event = threading.Event()
        data_store = MemoryDataStore()
        worker = Worker(data_store)
        worker.subscribe(listener)
        with worker:
            job = Job('task_id', fail_func, args=(), kwargs={}, schedule_id='foo',
                      scheduled_fire_time=scheduled_start_time,
                      start_deadline=datetime(2020, 9, 14, 1, tzinfo=timezone.utc))
            worker.data_store.add_job(job)
            event.wait(5)

        # The worker was first started
        received_event = received_events.pop(0)
        assert isinstance(received_event, WorkerStarted)

        # Then a job was added
        received_event = received_events.pop(0)
        assert isinstance(received_event, JobAdded)
        assert received_event.job_id == job.id
        assert received_event.task_id == 'task_id'
        assert received_event.schedule_id == 'foo'

        # Then the deadline was missed
        received_event = received_events.pop(0)
        assert isinstance(received_event, JobDeadlineMissed)
        assert received_event.job_id == job.id
        assert received_event.task_id == 'task_id'
        assert received_event.schedule_id == 'foo'

        # Finally, the worker was stopped
        received_event = received_events.pop(0)
        assert isinstance(received_event, WorkerStopped)

        # There should be no more events on the list
        assert not received_events
