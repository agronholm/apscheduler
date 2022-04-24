from __future__ import annotations

import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from uuid import UUID

import anyio
import pytest
from anyio import fail_after
from pytest_mock import MockerFixture

from apscheduler.context import current_scheduler, current_worker, job_info
from apscheduler.enums import JobOutcome
from apscheduler.events import (
    Event,
    JobAdded,
    ScheduleAdded,
    ScheduleRemoved,
    SchedulerStarted,
    SchedulerStopped,
    TaskAdded,
)
from apscheduler.exceptions import JobLookupError
from apscheduler.schedulers.async_ import AsyncScheduler
from apscheduler.schedulers.sync import Scheduler
from apscheduler.structures import Job, Task
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

if sys.version_info >= (3, 9):
    from zoneinfo import ZoneInfo
else:
    from backports.zoneinfo import ZoneInfo

pytestmark = pytest.mark.anyio


async def dummy_async_job(delay: float = 0, fail: bool = False) -> str:
    await anyio.sleep(delay)
    if fail:
        raise RuntimeError("failing as requested")
    else:
        return "returnvalue"


def dummy_sync_job(delay: float = 0, fail: bool = False) -> str:
    time.sleep(delay)
    if fail:
        raise RuntimeError("failing as requested")
    else:
        return "returnvalue"


class TestAsyncScheduler:
    async def test_schedule_job(self) -> None:
        def listener(received_event: Event) -> None:
            received_events.append(received_event)
            if len(received_events) == 5:
                event.set()

        received_events: list[Event] = []
        event = anyio.Event()
        scheduler = AsyncScheduler(start_worker=False)
        scheduler.events.subscribe(listener)
        trigger = DateTrigger(datetime.now(timezone.utc))
        async with scheduler:
            await scheduler.add_schedule(dummy_async_job, trigger, id="foo")
            with fail_after(3):
                await event.wait()

        # The scheduler was first started
        received_event = received_events.pop(0)
        assert isinstance(received_event, SchedulerStarted)

        # Then the task was added
        received_event = received_events.pop(0)
        assert isinstance(received_event, TaskAdded)
        assert received_event.task_id == "test_schedulers:dummy_async_job"

        # Then a schedule was added
        received_event = received_events.pop(0)
        assert isinstance(received_event, ScheduleAdded)
        assert received_event.schedule_id == "foo"
        # assert received_event.task_id == 'task_id'

        # Then that schedule was processed and a job was added for it
        received_event = received_events.pop(0)
        assert isinstance(received_event, JobAdded)
        assert received_event.schedule_id == "foo"
        assert received_event.task_id == "test_schedulers:dummy_async_job"

        # Then the schedule was removed since the trigger had been exhausted
        received_event = received_events.pop(0)
        assert isinstance(received_event, ScheduleRemoved)
        assert received_event.schedule_id == "foo"

        # Finally, the scheduler was stopped
        received_event = received_events.pop(0)
        assert isinstance(received_event, SchedulerStopped)

        # There should be no more events on the list
        assert not received_events

    @pytest.mark.parametrize(
        "max_jitter, expected_upper_bound",
        [pytest.param(2, 2, id="within"), pytest.param(4, 2.999999, id="exceed")],
    )
    async def test_jitter(
        self,
        mocker: MockerFixture,
        timezone: ZoneInfo,
        max_jitter: float,
        expected_upper_bound: float,
    ) -> None:
        job_id: UUID | None = None

        def job_added_listener(event: Event) -> None:
            nonlocal job_id
            assert isinstance(event, JobAdded)
            job_id = event.job_id
            job_added_event.set()

        jitter = 1.569374
        orig_start_time = datetime.now(timezone) - timedelta(seconds=1)
        fake_uniform = mocker.patch("random.uniform")
        fake_uniform.configure_mock(side_effect=lambda a, b: jitter)
        async with AsyncScheduler(start_worker=False) as scheduler:
            trigger = IntervalTrigger(seconds=3, start_time=orig_start_time)
            job_added_event = anyio.Event()
            scheduler.events.subscribe(job_added_listener, {JobAdded})
            schedule_id = await scheduler.add_schedule(
                dummy_async_job, trigger, max_jitter=max_jitter
            )
            schedule = await scheduler.get_schedule(schedule_id)
            assert schedule.max_jitter == timedelta(seconds=max_jitter)

            # Wait for the job to be added
            with fail_after(3):
                await job_added_event.wait()

            fake_uniform.assert_called_once_with(0, expected_upper_bound)

            # Check that the job was created with the proper amount of jitter in its scheduled time
            jobs = await scheduler.data_store.get_jobs({job_id})
            assert jobs[0].jitter == timedelta(seconds=jitter)
            assert jobs[0].scheduled_fire_time == orig_start_time + timedelta(
                seconds=jitter
            )
            assert jobs[0].original_scheduled_time == orig_start_time

    async def test_get_job_result_success(self) -> None:
        async with AsyncScheduler() as scheduler:
            job_id = await scheduler.add_job(dummy_async_job, kwargs={"delay": 0.2})
            result = await scheduler.get_job_result(job_id)
            assert result.job_id == job_id
            assert result.outcome is JobOutcome.success
            assert result.return_value == "returnvalue"

    async def test_get_job_result_error(self) -> None:
        async with AsyncScheduler() as scheduler:
            job_id = await scheduler.add_job(
                dummy_async_job, kwargs={"delay": 0.2, "fail": True}
            )
            result = await scheduler.get_job_result(job_id)
            assert result.job_id == job_id
            assert result.outcome is JobOutcome.error
            assert isinstance(result.exception, RuntimeError)
            assert str(result.exception) == "failing as requested"

    async def test_get_job_result_nowait_not_yet_ready(self) -> None:
        async with AsyncScheduler() as scheduler:
            job_id = await scheduler.add_job(dummy_async_job, kwargs={"delay": 0.2})
            with pytest.raises(JobLookupError):
                await scheduler.get_job_result(job_id, wait=False)

    async def test_run_job_success(self) -> None:
        async with AsyncScheduler() as scheduler:
            return_value = await scheduler.run_job(dummy_async_job)
            assert return_value == "returnvalue"

    async def test_run_job_failure(self) -> None:
        async with AsyncScheduler() as scheduler:
            with pytest.raises(RuntimeError, match="failing as requested"):
                await scheduler.run_job(dummy_async_job, kwargs={"fail": True})

    async def test_contextvars(self) -> None:
        def check_contextvars() -> None:
            assert current_scheduler.get() is scheduler
            assert current_worker.get() is scheduler.worker
            info = job_info.get()
            assert info.task_id == "task_id"
            assert info.schedule_id == "foo"
            assert info.scheduled_fire_time == scheduled_fire_time
            assert info.jitter == timedelta(seconds=2.16)
            assert info.start_deadline == start_deadline
            assert info.tags == {"foo", "bar"}

        scheduled_fire_time = datetime.now(timezone.utc)
        start_deadline = datetime.now(timezone.utc) + timedelta(seconds=10)
        async with AsyncScheduler() as scheduler:
            await scheduler.data_store.add_task(
                Task(id="task_id", func=check_contextvars)
            )
            job = Job(
                task_id="task_id",
                schedule_id="foo",
                scheduled_fire_time=scheduled_fire_time,
                jitter=timedelta(seconds=2.16),
                start_deadline=start_deadline,
                tags={"foo", "bar"},
            )
            await scheduler.data_store.add_job(job)
            result = await scheduler.get_job_result(job.id)
            if result.outcome is JobOutcome.error:
                raise result.exception
            else:
                assert result.outcome is JobOutcome.success


class TestSyncScheduler:
    def test_schedule_job(self):
        def listener(received_event: Event) -> None:
            received_events.append(received_event)
            if len(received_events) == 5:
                event.set()

        received_events: list[Event] = []
        event = threading.Event()
        scheduler = Scheduler(start_worker=False)
        scheduler.events.subscribe(listener)
        trigger = DateTrigger(datetime.now(timezone.utc))
        with scheduler:
            scheduler.add_schedule(dummy_sync_job, trigger, id="foo")
            event.wait(3)

        # The scheduler was first started
        received_event = received_events.pop(0)
        assert isinstance(received_event, SchedulerStarted)

        # Then the task was added
        received_event = received_events.pop(0)
        assert isinstance(received_event, TaskAdded)
        assert received_event.task_id == "test_schedulers:dummy_sync_job"

        # Then a schedule was added
        received_event = received_events.pop(0)
        assert isinstance(received_event, ScheduleAdded)
        assert received_event.schedule_id == "foo"

        # Then that schedule was processed and a job was added for it
        received_event = received_events.pop(0)
        assert isinstance(received_event, JobAdded)
        assert received_event.schedule_id == "foo"
        assert received_event.task_id == "test_schedulers:dummy_sync_job"

        # Then the schedule was removed since the trigger had been exhausted
        received_event = received_events.pop(0)
        assert isinstance(received_event, ScheduleRemoved)
        assert received_event.schedule_id == "foo"

        # Finally, the scheduler was stopped
        received_event = received_events.pop(0)
        assert isinstance(received_event, SchedulerStopped)

        # There should be no more events on the list
        assert not received_events

    @pytest.mark.parametrize(
        "max_jitter, expected_upper_bound",
        [pytest.param(2, 2, id="within"), pytest.param(4, 2.999999, id="exceed")],
    )
    def test_jitter(
        self,
        mocker: MockerFixture,
        timezone: ZoneInfo,
        max_jitter: float,
        expected_upper_bound: float,
    ) -> None:
        job_id: UUID | None = None

        def job_added_listener(event: Event) -> None:
            nonlocal job_id
            assert isinstance(event, JobAdded)
            job_id = event.job_id
            job_added_event.set()

        jitter = 1.569374
        orig_start_time = datetime.now(timezone) - timedelta(seconds=1)
        fake_uniform = mocker.patch("random.uniform")
        fake_uniform.configure_mock(side_effect=lambda a, b: jitter)
        with Scheduler(start_worker=False) as scheduler:
            trigger = IntervalTrigger(seconds=3, start_time=orig_start_time)
            job_added_event = threading.Event()
            scheduler.events.subscribe(job_added_listener, {JobAdded})
            schedule_id = scheduler.add_schedule(
                dummy_async_job, trigger, max_jitter=max_jitter
            )
            schedule = scheduler.get_schedule(schedule_id)
            assert schedule.max_jitter == timedelta(seconds=max_jitter)

            # Wait for the job to be added
            job_added_event.wait(3)

            fake_uniform.assert_called_once_with(0, expected_upper_bound)

            # Check that the job was created with the proper amount of jitter in its scheduled time
            jobs = scheduler.data_store.get_jobs({job_id})
            assert jobs[0].jitter == timedelta(seconds=jitter)
            assert jobs[0].scheduled_fire_time == orig_start_time + timedelta(
                seconds=jitter
            )
            assert jobs[0].original_scheduled_time == orig_start_time

    def test_get_job_result(self) -> None:
        with Scheduler() as scheduler:
            job_id = scheduler.add_job(dummy_sync_job)
            result = scheduler.get_job_result(job_id)
            assert result.outcome is JobOutcome.success
            assert result.return_value == "returnvalue"

    def test_get_job_result_error(self) -> None:
        with Scheduler() as scheduler:
            job_id = scheduler.add_job(
                dummy_sync_job, kwargs={"delay": 0.2, "fail": True}
            )
            result = scheduler.get_job_result(job_id)
            assert result.job_id == job_id
            assert result.outcome is JobOutcome.error
            assert isinstance(result.exception, RuntimeError)
            assert str(result.exception) == "failing as requested"

    def test_get_job_result_nowait_not_yet_ready(self) -> None:
        with Scheduler() as scheduler:
            job_id = scheduler.add_job(dummy_sync_job, kwargs={"delay": 0.2})
            with pytest.raises(JobLookupError):
                scheduler.get_job_result(job_id, wait=False)

    def test_run_job_success(self) -> None:
        with Scheduler() as scheduler:
            return_value = scheduler.run_job(dummy_sync_job)
            assert return_value == "returnvalue"

    def test_run_job_failure(self) -> None:
        with Scheduler() as scheduler:
            with pytest.raises(RuntimeError, match="failing as requested"):
                scheduler.run_job(dummy_sync_job, kwargs={"fail": True})

    def test_contextvars(self) -> None:
        def check_contextvars() -> None:
            assert current_scheduler.get() is scheduler
            assert current_worker.get() is scheduler.worker
            info = job_info.get()
            assert info.task_id == "task_id"
            assert info.schedule_id == "foo"
            assert info.scheduled_fire_time == scheduled_fire_time
            assert info.jitter == timedelta(seconds=2.16)
            assert info.start_deadline == start_deadline
            assert info.tags == {"foo", "bar"}

        scheduled_fire_time = datetime.now(timezone.utc)
        start_deadline = datetime.now(timezone.utc) + timedelta(seconds=10)
        with Scheduler() as scheduler:
            scheduler.data_store.add_task(Task(id="task_id", func=check_contextvars))
            job = Job(
                task_id="task_id",
                schedule_id="foo",
                scheduled_fire_time=scheduled_fire_time,
                jitter=timedelta(seconds=2.16),
                start_deadline=start_deadline,
                tags={"foo", "bar"},
            )
            scheduler.data_store.add_job(job)
            result = scheduler.get_job_result(job.id)
            if result.outcome is JobOutcome.error:
                raise result.exception
            else:
                assert result.outcome is JobOutcome.success
