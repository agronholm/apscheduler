from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Callable

import anyio
import pytest
from anyio import fail_after

from apscheduler import (
    Event,
    Job,
    JobAcquired,
    JobAdded,
    JobOutcome,
    JobReleased,
    TaskAdded,
    WorkerStopped,
)
from apscheduler._structures import Task
from apscheduler.datastores.memory import MemoryDataStore
from apscheduler.workers.async_ import AsyncWorker
from apscheduler.workers.sync import Worker

pytestmark = pytest.mark.anyio


def sync_func(*args, fail: bool, **kwargs):
    if fail:
        raise Exception("failing as requested")
    else:
        return args, kwargs


async def async_func(*args, fail: bool, **kwargs):
    if fail:
        raise Exception("failing as requested")
    else:
        return args, kwargs


def fail_func():
    pytest.fail("This function should never be run")


class TestAsyncWorker:
    @pytest.mark.parametrize(
        "target_func", [sync_func, async_func], ids=["sync", "async"]
    )
    @pytest.mark.parametrize("fail", [False, True], ids=["success", "fail"])
    async def test_run_job_nonscheduled_success(
        self, target_func: Callable, fail: bool
    ) -> None:
        def listener(received_event: Event):
            received_events.append(received_event)
            if isinstance(received_event, JobReleased):
                event.set()

        received_events: list[Event] = []
        event = anyio.Event()
        async with AsyncWorker(MemoryDataStore()) as worker:
            worker.event_broker.subscribe(listener)
            await worker.data_store.add_task(Task(id="task_id", func=target_func))
            job = Job(task_id="task_id", args=(1, 2), kwargs={"x": "foo", "fail": fail})
            await worker.data_store.add_job(job)
            with fail_after(3):
                await event.wait()

        # First, a task was added
        received_event = received_events.pop(0)
        assert isinstance(received_event, TaskAdded)
        assert received_event.task_id == "task_id"

        # Then a job was added
        received_event = received_events.pop(0)
        assert isinstance(received_event, JobAdded)
        assert received_event.job_id == job.id
        assert received_event.task_id == "task_id"
        assert received_event.schedule_id is None

        # Then the job was started
        received_event = received_events.pop(0)
        assert isinstance(received_event, JobAcquired)
        assert received_event.job_id == job.id
        assert received_event.worker_id == worker.identity

        received_event = received_events.pop(0)
        if fail:
            # Then the job failed
            assert isinstance(received_event, JobReleased)
            assert received_event.outcome is JobOutcome.error
            assert received_event.exception_type == "Exception"
            assert received_event.exception_message == "failing as requested"
            assert isinstance(received_event.exception_traceback, list)
            assert all(
                isinstance(line, str) for line in received_event.exception_traceback
            )
        else:
            # Then the job finished successfully
            assert isinstance(received_event, JobReleased)
            assert received_event.outcome is JobOutcome.success
            assert received_event.exception_type is None
            assert received_event.exception_message is None
            assert received_event.exception_traceback is None

        # Finally, the worker was stopped
        received_event = received_events.pop(0)
        assert isinstance(received_event, WorkerStopped)

        # There should be no more events on the list
        assert not received_events

    async def test_run_deadline_missed(self) -> None:
        def listener(received_event: Event):
            received_events.append(received_event)
            if isinstance(received_event, JobReleased):
                event.set()

        scheduled_start_time = datetime(2020, 9, 14, tzinfo=timezone.utc)
        received_events: list[Event] = []
        event = anyio.Event()
        async with AsyncWorker(MemoryDataStore()) as worker:
            worker.event_broker.subscribe(listener)
            await worker.data_store.add_task(Task(id="task_id", func=fail_func))
            job = Job(
                task_id="task_id",
                schedule_id="foo",
                scheduled_fire_time=scheduled_start_time,
                start_deadline=datetime(2020, 9, 14, 1, tzinfo=timezone.utc),
            )
            await worker.data_store.add_job(job)
            with fail_after(3):
                await event.wait()

        # First, a task was added
        received_event = received_events.pop(0)
        assert isinstance(received_event, TaskAdded)
        assert received_event.task_id == "task_id"

        # Then a job was added
        received_event = received_events.pop(0)
        assert isinstance(received_event, JobAdded)
        assert received_event.job_id == job.id
        assert received_event.task_id == "task_id"
        assert received_event.schedule_id == "foo"

        # The worker acquired the job
        received_event = received_events.pop(0)
        assert isinstance(received_event, JobAcquired)
        assert received_event.job_id == job.id
        assert received_event.worker_id == worker.identity

        # The worker determined that the deadline has been missed
        received_event = received_events.pop(0)
        assert isinstance(received_event, JobReleased)
        assert received_event.outcome is JobOutcome.missed_start_deadline
        assert received_event.job_id == job.id
        assert received_event.worker_id == worker.identity

        # Finally, the worker was stopped
        received_event = received_events.pop(0)
        assert isinstance(received_event, WorkerStopped)

        # There should be no more events on the list
        assert not received_events


class TestSyncWorker:
    @pytest.mark.parametrize("fail", [False, True], ids=["success", "fail"])
    def test_run_job_nonscheduled(self, fail: bool) -> None:
        def listener(received_event: Event):
            received_events.append(received_event)
            if isinstance(received_event, JobReleased):
                event.set()

        received_events: list[Event] = []
        event = threading.Event()
        with Worker(MemoryDataStore()) as worker:
            worker.event_broker.subscribe(listener)
            worker.data_store.add_task(Task(id="task_id", func=sync_func))
            job = Job(task_id="task_id", args=(1, 2), kwargs={"x": "foo", "fail": fail})
            worker.data_store.add_job(job)
            event.wait(3)

        # First, a task was added
        received_event = received_events.pop(0)
        assert isinstance(received_event, TaskAdded)
        assert received_event.task_id == "task_id"

        # Then a job was added
        received_event = received_events.pop(0)
        assert isinstance(received_event, JobAdded)
        assert received_event.job_id == job.id
        assert received_event.task_id == "task_id"
        assert received_event.schedule_id is None

        # Then the job was started
        received_event = received_events.pop(0)
        assert isinstance(received_event, JobAcquired)
        assert received_event.job_id == job.id
        assert received_event.worker_id == worker.identity

        received_event = received_events.pop(0)
        if fail:
            # Then the job failed
            assert isinstance(received_event, JobReleased)
            assert received_event.outcome is JobOutcome.error
            assert received_event.exception_type == "Exception"
            assert received_event.exception_message == "failing as requested"
            assert isinstance(received_event.exception_traceback, list)
            assert all(
                isinstance(line, str) for line in received_event.exception_traceback
            )
        else:
            # Then the job finished successfully
            assert isinstance(received_event, JobReleased)
            assert received_event.outcome is JobOutcome.success
            assert received_event.exception_type is None
            assert received_event.exception_message is None
            assert received_event.exception_traceback is None

        # Finally, the worker was stopped
        received_event = received_events.pop(0)
        assert isinstance(received_event, WorkerStopped)

        # There should be no more events on the list
        assert not received_events

    def test_run_deadline_missed(self) -> None:
        def listener(received_event: Event):
            received_events.append(received_event)
            if isinstance(received_event, JobReleased):
                event.set()

        scheduled_start_time = datetime(2020, 9, 14, tzinfo=timezone.utc)
        received_events: list[Event] = []
        event = threading.Event()
        with Worker(MemoryDataStore()) as worker:
            worker.event_broker.subscribe(listener)
            worker.data_store.add_task(Task(id="task_id", func=fail_func))
            job = Job(
                task_id="task_id",
                schedule_id="foo",
                scheduled_fire_time=scheduled_start_time,
                start_deadline=datetime(2020, 9, 14, 1, tzinfo=timezone.utc),
            )
            worker.data_store.add_job(job)
            event.wait(3)

        # First, a task was added
        received_event = received_events.pop(0)
        assert isinstance(received_event, TaskAdded)
        assert received_event.task_id == "task_id"

        # Then a job was added
        received_event = received_events.pop(0)
        assert isinstance(received_event, JobAdded)
        assert received_event.job_id == job.id
        assert received_event.task_id == "task_id"
        assert received_event.schedule_id == "foo"

        # The worker acquired the job
        received_event = received_events.pop(0)
        assert isinstance(received_event, JobAcquired)
        assert received_event.job_id == job.id
        assert received_event.worker_id == worker.identity

        # The worker determined that the deadline has been missed
        received_event = received_events.pop(0)
        assert isinstance(received_event, JobReleased)
        assert received_event.outcome is JobOutcome.missed_start_deadline
        assert received_event.job_id == job.id
        assert received_event.worker_id == worker.identity

        # Finally, the worker was stopped
        received_event = received_events.pop(0)
        assert isinstance(received_event, WorkerStopped)

        # There should be no more events on the list
        assert not received_events
