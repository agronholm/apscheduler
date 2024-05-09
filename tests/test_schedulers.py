from __future__ import annotations

import os
import subprocess
import sys
import sysconfig
import threading
import time
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from functools import partial
from pathlib import Path
from queue import Queue
from types import ModuleType

import anyio
import pytest
from anyio import WouldBlock, create_memory_object_stream, fail_after, sleep
from pytest import MonkeyPatch
from pytest_mock import MockerFixture, MockFixture

from apscheduler import (
    AsyncScheduler,
    CoalescePolicy,
    Event,
    Job,
    JobAcquired,
    JobAdded,
    JobLookupError,
    JobOutcome,
    JobReleased,
    RunState,
    ScheduleAdded,
    ScheduleLookupError,
    Scheduler,
    ScheduleRemoved,
    SchedulerRole,
    SchedulerStarted,
    SchedulerStopped,
    ScheduleUpdated,
    TaskAdded,
    TaskUpdated,
    current_async_scheduler,
    current_job,
)
from apscheduler.abc import DataStore
from apscheduler.datastores.memory import MemoryDataStore
from apscheduler.eventbrokers.local import LocalEventBroker
from apscheduler.executors.async_ import AsyncJobExecutor
from apscheduler.executors.subprocess import ProcessPoolJobExecutor
from apscheduler.executors.thread import ThreadPoolJobExecutor
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

if sys.version_info >= (3, 11):
    from datetime import UTC
else:
    UTC = timezone.utc

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


class DummyClass:
    def __init__(self, value: int):
        self.value = value

    @staticmethod
    async def dummy_static_method() -> str:
        return "static"

    @staticmethod
    async def dummy_async_static_method() -> str:
        return "static"

    @classmethod
    def dummy_class_method(cls) -> str:
        return "class"

    @classmethod
    async def dummy_async_class_method(cls) -> str:
        return "class"

    def dummy_instance_method(self) -> int:
        return self.value

    async def dummy_async_instance_method(self) -> int:
        return self.value

    def __call__(self) -> int:
        return self.value


class TestAsyncScheduler:
    async def test_bad_default_executor(self) -> None:
        with pytest.raises(
            ValueError,
            match=r"default_job_executor must be one of the given job "
            r"executors \(async, threadpool, processpool\)",
        ):
            AsyncScheduler(default_job_executor="foo")

    async def test_use_before_initialized(self) -> None:
        scheduler = AsyncScheduler()
        with pytest.raises(
            RuntimeError, match="The scheduler has not been initialized yet"
        ):
            await scheduler.add_job(dummy_async_job)

    async def test_properties(self) -> None:
        async with AsyncScheduler() as scheduler:
            assert isinstance(scheduler.data_store, MemoryDataStore)
            assert isinstance(scheduler.event_broker, LocalEventBroker)
            assert scheduler.role is SchedulerRole.both
            assert isinstance(scheduler.identity, str)
            assert len(scheduler.job_executors) == 3
            assert isinstance(scheduler.job_executors["async"], AsyncJobExecutor)
            assert isinstance(
                scheduler.job_executors["threadpool"], ThreadPoolJobExecutor
            )
            assert isinstance(
                scheduler.job_executors["processpool"], ProcessPoolJobExecutor
            )
            assert scheduler.default_job_executor == "async"
            assert scheduler.state is RunState.stopped

    @pytest.mark.parametrize("as_default", [False, True])
    async def test_async_executor(self, as_default: bool) -> None:
        async with AsyncScheduler() as scheduler:
            await scheduler.start_in_background()
            if as_default:
                thread_id = await scheduler.run_job(threading.get_ident)
            else:
                thread_id = await scheduler.run_job(
                    threading.get_ident, job_executor="async"
                )

            assert thread_id == threading.get_ident()

    async def test_threadpool_executor(self) -> None:
        async with AsyncScheduler() as scheduler:
            await scheduler.start_in_background()
            thread_id = await scheduler.run_job(
                threading.get_ident, job_executor="threadpool"
            )
            assert thread_id != threading.get_ident()

    async def test_processpool_executor(self) -> None:
        async with AsyncScheduler() as scheduler:
            await scheduler.start_in_background()
            pid = await scheduler.run_job(os.getpid, job_executor="processpool")
            assert pid != os.getpid()

    async def test_configure_task(self, raw_datastore: DataStore) -> None:
        send, receive = create_memory_object_stream[Event](2)
        async with AsyncScheduler(data_store=raw_datastore) as scheduler:
            scheduler.subscribe(send.send)
            await scheduler.configure_task("mytask", func=dummy_async_job)
            await scheduler.configure_task("mytask", misfire_grace_time=2)
            tasks = await scheduler.get_tasks()
            assert len(tasks) == 1
            assert tasks[0].id == "mytask"
            assert tasks[0].func == f"{__name__}:dummy_async_job"
            assert tasks[0].misfire_grace_time == timedelta(seconds=2)

        with fail_after(3):
            event = await receive.receive()
            assert isinstance(event, TaskAdded)
            assert event.task_id == "mytask"

            event = await receive.receive()
            assert isinstance(event, TaskUpdated)
            assert event.task_id == "mytask"

    async def test_add_pause_unpause_remove_schedule(
        self, raw_datastore: DataStore, timezone: ZoneInfo
    ) -> None:
        send, receive = create_memory_object_stream[Event](5)
        async with AsyncScheduler(data_store=raw_datastore) as scheduler:
            scheduler.subscribe(send.send)
            now = datetime.now(timezone)
            trigger = DateTrigger(now)
            schedule_id = await scheduler.add_schedule(
                dummy_async_job, trigger, id="foo"
            )
            assert schedule_id == "foo"

            schedules = await scheduler.get_schedules()
            assert len(schedules) == 1
            assert schedules[0].id == "foo"
            assert schedules[0].task_id == f"{__name__}:dummy_async_job"

            await scheduler.pause_schedule("foo")
            schedule = await scheduler.get_schedule("foo")
            assert schedule.paused
            assert schedule.next_fire_time == now

            await scheduler.unpause_schedule("foo")
            schedule = await scheduler.get_schedule("foo")
            assert not schedule.paused
            assert schedule.next_fire_time == now

            await scheduler.remove_schedule(schedule_id)
            assert not await scheduler.get_schedules()

        with fail_after(3):
            event = await receive.receive()
            assert isinstance(event, TaskAdded)
            assert event.task_id == f"{__name__}:dummy_async_job"

            event = await receive.receive()
            assert isinstance(event, ScheduleAdded)
            assert event.schedule_id == "foo"
            assert event.task_id == f"{__name__}:dummy_async_job"
            assert event.next_fire_time == now

            event = await receive.receive()
            assert isinstance(event, ScheduleUpdated)
            assert event.schedule_id == "foo"
            assert event.task_id == f"{__name__}:dummy_async_job"
            assert event.next_fire_time == now

            event = await receive.receive()
            assert isinstance(event, ScheduleUpdated)
            assert event.schedule_id == "foo"
            assert event.task_id == f"{__name__}:dummy_async_job"
            assert event.next_fire_time == now

            event = await receive.receive()
            assert isinstance(event, ScheduleRemoved)
            assert event.schedule_id == "foo"
            assert event.task_id == f"{__name__}:dummy_async_job"
            assert not event.finished

    async def test_add_job_wait_result(self, raw_datastore: DataStore) -> None:
        send, receive = create_memory_object_stream[Event](2)
        async with AsyncScheduler(data_store=raw_datastore) as scheduler:
            assert await scheduler.get_jobs() == []

            scheduler.subscribe(send.send)
            job_id = await scheduler.add_job(dummy_async_job, result_expiration_time=10)

            with fail_after(3):
                event = await receive.receive()
                assert isinstance(event, TaskAdded)
                assert event.task_id == f"{__name__}:dummy_async_job"

                event = await receive.receive()
                assert isinstance(event, JobAdded)
                assert event.job_id == job_id

            jobs = await scheduler.get_jobs()
            assert len(jobs) == 1
            assert jobs[0].id == job_id
            assert jobs[0].task_id == f"{__name__}:dummy_async_job"

            with pytest.raises(JobLookupError):
                await scheduler.get_job_result(job_id, wait=False)

            await scheduler.start_in_background()

            with fail_after(3):
                event = await receive.receive()
                assert isinstance(event, SchedulerStarted)

                event = await receive.receive()
                assert isinstance(event, JobAcquired)
                assert event.job_id == job_id
                assert event.task_id == f"{__name__}:dummy_async_job"
                assert event.schedule_id is None

                event = await receive.receive()
                assert isinstance(event, JobReleased)
                assert event.job_id == job_id
                assert event.task_id == f"{__name__}:dummy_async_job"
                assert event.schedule_id is None
                assert event.outcome is JobOutcome.success

                result = await scheduler.get_job_result(job_id)
                assert result.outcome is JobOutcome.success
                assert result.return_value == "returnvalue"

    @pytest.mark.parametrize("success", [True, False])
    async def test_run_job(self, raw_datastore: DataStore, success: bool) -> None:
        send, receive = create_memory_object_stream[Event](4)
        async with AsyncScheduler(data_store=raw_datastore) as scheduler:
            await scheduler.start_in_background()
            scheduler.subscribe(send.send)
            try:
                result = await scheduler.run_job(
                    dummy_async_job, kwargs={"fail": not success}
                )
            except RuntimeError as exc:
                assert str(exc) == "failing as requested"
            else:
                assert result == "returnvalue"

            assert not await scheduler.get_jobs()

            with fail_after(3):
                # The task was added
                event = await receive.receive()
                assert isinstance(event, TaskAdded)
                assert event.task_id == f"{__name__}:dummy_async_job"

                # The job was added
                event = await receive.receive()
                assert isinstance(event, JobAdded)
                job_id = event.job_id
                assert event.task_id == f"{__name__}:dummy_async_job"

                # The scheduler acquired the job
                event = await receive.receive()
                assert isinstance(event, JobAcquired)
                assert event.job_id == job_id
                assert event.task_id == f"{__name__}:dummy_async_job"
                assert event.schedule_id is None
                assert event.scheduler_id == scheduler.identity

                # The scheduler released the job
                event = await receive.receive()
                assert isinstance(event, JobReleased)
                assert event.job_id == job_id
                assert event.task_id == f"{__name__}:dummy_async_job"
                assert event.schedule_id is None
                assert event.scheduler_id == scheduler.identity

        # The scheduler was stopped
        event = await receive.receive()
        assert isinstance(event, SchedulerStopped)

        # There should be no more events on the list
        with pytest.raises(WouldBlock):
            receive.receive_nowait()

    @pytest.mark.parametrize(
        "target, expected_result",
        [
            pytest.param(dummy_async_job, "returnvalue", id="async_func"),
            pytest.param(dummy_sync_job, "returnvalue", id="sync_func"),
            pytest.param(DummyClass.dummy_static_method, "static", id="staticmethod"),
            pytest.param(
                DummyClass.dummy_async_static_method, "static", id="async_staticmethod"
            ),
            pytest.param(DummyClass.dummy_class_method, "class", id="classmethod"),
            pytest.param(
                DummyClass.dummy_async_class_method, "class", id="async_classmethod"
            ),
            pytest.param(DummyClass(5).dummy_instance_method, 5, id="instancemethod"),
            pytest.param(
                DummyClass(6).dummy_async_instance_method, 6, id="async_instancemethod"
            ),
            pytest.param(bytes, b"", id="builtin_function"),
            pytest.param(
                datetime(2023, 10, 19, tzinfo=UTC).timestamp,
                1697673600.0,
                id="builtin_method",
            ),
            pytest.param(partial(bytes, "foo", "ascii"), b"foo", id="partial"),
        ],
    )
    @pytest.mark.parametrize(
        "use_scheduling",
        [
            pytest.param(False, id="job"),
            pytest.param(True, id="schedule"),
        ],
    )
    async def test_callable_types(
        self,
        target: Callable,
        expected_result: object,
        use_scheduling: bool,
        raw_datastore: DataStore,
        timezone: ZoneInfo,
    ) -> None:
        send, receive = create_memory_object_stream[Event](4)
        now = datetime.now(timezone)
        async with AsyncScheduler(data_store=raw_datastore) as scheduler:
            scheduler.subscribe(send.send, {JobReleased})
            await scheduler.start_in_background()
            if use_scheduling:
                trigger = DateTrigger(now)
                await scheduler.add_schedule(target, trigger, id="foo")
            else:
                await scheduler.add_job(target, result_expiration_time=10)

            with fail_after(3):
                event = await receive.receive()
                assert isinstance(event, JobReleased)

            if not use_scheduling:
                result = await scheduler.get_job_result(event.job_id)
                assert result.outcome is JobOutcome.success
                assert result.return_value == expected_result

    async def test_scheduled_job_missed_deadline(
        self, raw_datastore: DataStore, timezone: ZoneInfo
    ) -> None:
        send, receive = create_memory_object_stream[Event](4)
        trigger = DateTrigger(datetime.now(timezone) - timedelta(seconds=1))
        async with AsyncScheduler(data_store=raw_datastore) as scheduler:
            await scheduler.add_schedule(
                dummy_async_job, trigger, misfire_grace_time=0, id="foo"
            )
            scheduler.subscribe(send.send)
            await scheduler.start_in_background()

            with fail_after(3):
                # The scheduler was started
                event = await receive.receive()
                assert isinstance(event, SchedulerStarted)

                # The schedule was processed and a job was added for it
                event = await receive.receive()
                assert isinstance(event, JobAdded)
                assert event.schedule_id == "foo"
                assert event.task_id == "test_schedulers:dummy_async_job"

                # The schedule was updated with a null next fire time
                event = await receive.receive()
                assert isinstance(event, ScheduleUpdated)
                assert event.schedule_id == "foo"
                assert event.next_fire_time is None

                # The new job was acquired
                event = await receive.receive()
                assert isinstance(event, JobAcquired)
                job_id = event.job_id
                assert event.task_id == "test_schedulers:dummy_async_job"
                assert event.schedule_id == "foo"

                # The new job was released
                event = await receive.receive()
                assert isinstance(event, JobReleased)
                assert event.job_id == job_id
                assert event.task_id == "test_schedulers:dummy_async_job"
                assert event.schedule_id == "foo"
                assert event.outcome is JobOutcome.missed_start_deadline

        # The scheduler was stopped
        event = await receive.receive()
        assert isinstance(event, SchedulerStopped)

        # There should be no more events on the list
        with pytest.raises(WouldBlock):
            receive.receive_nowait()

    @pytest.mark.parametrize(
        "coalesce, expected_jobs, first_fire_time_delta",
        [
            pytest.param(
                CoalescePolicy.all, 4, timedelta(minutes=3, seconds=5), id="all"
            ),
            pytest.param(
                CoalescePolicy.earliest,
                1,
                timedelta(minutes=3, seconds=5),
                id="earliest",
            ),
            pytest.param(CoalescePolicy.latest, 1, timedelta(seconds=5), id="latest"),
        ],
    )
    async def test_coalesce_policy(
        self,
        coalesce: CoalescePolicy,
        expected_jobs: int,
        first_fire_time_delta: timedelta,
        raw_datastore: DataStore,
        timezone: ZoneInfo,
    ) -> None:
        send, receive = create_memory_object_stream[Event](4)
        now = datetime.now(timezone)
        first_start_time = now - timedelta(minutes=3, seconds=5)
        trigger = IntervalTrigger(minutes=1, start_time=first_start_time)
        async with AsyncScheduler(
            data_store=raw_datastore,
            role=SchedulerRole.scheduler,
            cleanup_interval=None,
        ) as scheduler:
            await scheduler.add_schedule(
                dummy_async_job, trigger, id="foo", coalesce=coalesce
            )
            scheduler.subscribe(send.send)
            await scheduler.start_in_background()

            with fail_after(3):
                # The scheduler was started
                event = await receive.receive()
                assert isinstance(event, SchedulerStarted)

                # The schedule was processed and one or more jobs weres added
                for index in range(expected_jobs):
                    event = await receive.receive()
                    assert isinstance(event, JobAdded)
                    assert event.schedule_id == "foo"
                    assert event.task_id == "test_schedulers:dummy_async_job"

                event = await receive.receive()
                assert isinstance(event, ScheduleUpdated)
                assert event.next_fire_time == now + timedelta(seconds=55)

            expected_scheduled_fire_time = now - first_fire_time_delta
            jobs = await scheduler.get_jobs()
            for job in sorted(jobs, key=lambda job: job.scheduled_fire_time):
                assert job.scheduled_fire_time
                assert job.scheduled_fire_time < now
                assert job.scheduled_fire_time == expected_scheduled_fire_time
                expected_scheduled_fire_time += timedelta(minutes=1)

        # The scheduler was stopped
        event = await receive.receive()
        assert isinstance(event, SchedulerStopped)

        # There should be no more events on the list
        with pytest.raises(WouldBlock):
            receive.receive_nowait()

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
        raw_datastore: DataStore,
    ) -> None:
        send, receive = create_memory_object_stream[Event](4)
        jitter = 1.569374
        now = datetime.now(timezone)
        fake_uniform = mocker.patch("random.uniform")
        fake_uniform.configure_mock(side_effect=lambda a, b: jitter)
        async with AsyncScheduler(
            data_store=raw_datastore,
            role=SchedulerRole.scheduler,
        ) as scheduler:
            scheduler.subscribe(send.send)
            trigger = DateTrigger(now)
            schedule_id = await scheduler.add_schedule(
                dummy_async_job, trigger, id="foo", max_jitter=max_jitter
            )
            schedule = await scheduler.get_schedule(schedule_id)
            assert schedule.max_jitter == timedelta(seconds=max_jitter)

            await scheduler.start_in_background()

            with fail_after(3):
                # The task was added
                event = await receive.receive()
                assert isinstance(event, TaskAdded)
                assert event.task_id == "test_schedulers:dummy_async_job"

                # The schedule was added
                event = await receive.receive()
                assert isinstance(event, ScheduleAdded)
                assert event.schedule_id == "foo"
                assert event.next_fire_time == now

                # The scheduler was started
                event = await receive.receive()
                assert isinstance(event, SchedulerStarted)

                # The schedule was processed and a job was added for it
                event = await receive.receive()
                assert isinstance(event, JobAdded)
                assert event.schedule_id == "foo"
                assert event.task_id == "test_schedulers:dummy_async_job"

                # Check that the job was created with the proper amount of jitter in its
                # scheduled time
                jobs = await scheduler.get_jobs()
                assert len(jobs) == 1
                assert jobs[0].jitter == timedelta(seconds=jitter)
                assert jobs[0].scheduled_fire_time == now + timedelta(seconds=jitter)
                assert jobs[0].original_scheduled_time == now

    async def test_add_job_get_result_success(self, raw_datastore: DataStore) -> None:
        async with AsyncScheduler(data_store=raw_datastore) as scheduler:
            job_id = await scheduler.add_job(
                dummy_async_job, kwargs={"delay": 0.2}, result_expiration_time=5
            )
            await scheduler.start_in_background()
            with fail_after(3):
                result = await scheduler.get_job_result(job_id)

            assert result.job_id == job_id
            assert result.outcome is JobOutcome.success
            assert result.return_value == "returnvalue"

    async def test_add_job_get_result_empty(self, raw_datastore: DataStore) -> None:
        send, receive = create_memory_object_stream[Event](4)
        async with AsyncScheduler(data_store=raw_datastore) as scheduler:
            await scheduler.start_in_background()

            scheduler.subscribe(send.send)
            job_id = await scheduler.add_job(dummy_async_job)

            with fail_after(3):
                event = await receive.receive()
                assert isinstance(event, TaskAdded)

                event = await receive.receive()
                assert isinstance(event, JobAdded)
                assert event.job_id == job_id

                event = await receive.receive()
                assert isinstance(event, JobAcquired)
                assert event.job_id == job_id
                assert event.task_id == "test_schedulers:dummy_async_job"
                assert event.schedule_id is None

                event = await receive.receive()
                assert isinstance(event, JobReleased)
                assert event.job_id == job_id
                assert event.task_id == "test_schedulers:dummy_async_job"
                assert event.schedule_id is None

            with pytest.raises(JobLookupError):
                await scheduler.get_job_result(job_id, wait=False)

    async def test_add_job_get_result_error(self) -> None:
        async with AsyncScheduler() as scheduler:
            job_id = await scheduler.add_job(
                dummy_async_job,
                kwargs={"delay": 0.2, "fail": True},
                result_expiration_time=5,
            )
            await scheduler.start_in_background()
            with fail_after(3):
                result = await scheduler.get_job_result(job_id)

            assert result.job_id == job_id
            assert result.outcome is JobOutcome.error
            assert isinstance(result.exception, RuntimeError)
            assert str(result.exception) == "failing as requested"

    async def test_add_job_get_result_no_ready_yet(self) -> None:
        send, receive = create_memory_object_stream[Event](4)
        async with AsyncScheduler() as scheduler:
            scheduler.subscribe(send.send)
            job_id = await scheduler.add_job(dummy_async_job, kwargs={"delay": 0.2})

            with fail_after(3):
                event = await receive.receive()
                assert isinstance(event, TaskAdded)

                event = await receive.receive()
                assert isinstance(event, JobAdded)
                assert event.job_id == job_id

            with pytest.raises(JobLookupError), fail_after(1):
                await scheduler.get_job_result(job_id, wait=False)

    async def test_contextvars(self, mocker: MockerFixture, timezone: ZoneInfo) -> None:
        def check_contextvars() -> None:
            assert current_async_scheduler.get() is scheduler
            info = current_job.get()
            assert isinstance(info, Job)
            assert info.task_id == "contextvars"
            assert info.schedule_id == "foo"
            assert info.original_scheduled_time == now
            assert info.scheduled_fire_time == now + timedelta(seconds=2.16)
            assert info.jitter == timedelta(seconds=2.16)
            assert info.start_deadline == now + timedelta(seconds=2.16) + timedelta(
                seconds=10
            )

        fake_uniform = mocker.patch("random.uniform")
        fake_uniform.configure_mock(return_value=2.16)
        send, receive = create_memory_object_stream[Event](1)
        now = datetime.now(timezone)
        async with AsyncScheduler() as scheduler:
            await scheduler.configure_task("contextvars", func=check_contextvars)
            await scheduler.add_schedule(
                "contextvars",
                DateTrigger(now),
                id="foo",
                max_jitter=3,
                misfire_grace_time=10,
            )
            scheduler.subscribe(send.send, {JobReleased})
            await scheduler.start_in_background()

            with fail_after(3):
                event = await receive.receive()

            assert event.outcome is JobOutcome.success

    async def test_explicit_cleanup(self, raw_datastore: DataStore) -> None:
        send, receive = create_memory_object_stream[Event](1)
        async with AsyncScheduler(raw_datastore, cleanup_interval=None) as scheduler:
            scheduler.subscribe(send.send, {ScheduleRemoved})
            event = anyio.Event()
            scheduler.subscribe(lambda _: event.set(), {JobReleased}, one_shot=True)
            await scheduler.start_in_background()

            # Add a job whose result expires after 1 ms
            job_id = await scheduler.add_job(
                dummy_async_job, result_expiration_time=0.001
            )
            with fail_after(3):
                await event.wait()

            # After the sleeping past the expiration time and performing a cleanup, the
            # result should not be there anymore
            await sleep(0.001)
            await scheduler.cleanup()
            with pytest.raises(JobLookupError):
                await scheduler.get_job_result(job_id)

            # Add a schedule to immediately set the event
            event = anyio.Event()
            scheduler.subscribe(lambda _: event.set(), {JobReleased}, one_shot=True)
            await scheduler.add_schedule(
                dummy_async_job, DateTrigger(datetime.now(timezone.utc)), id="event_set"
            )
            with fail_after(3):
                await event.wait()

            # The schedule should still be around, but with a null next_fire_time
            schedule = await scheduler.get_schedule("event_set")
            assert schedule.next_fire_time is None

            # After the cleanup, the schedule should be gone
            await scheduler.cleanup()
            with pytest.raises(ScheduleLookupError):
                await scheduler.get_schedule("event_set")

            # Check that the corresponding event was received
            with fail_after(3):
                event = await receive.receive()
                assert isinstance(event, ScheduleRemoved)
                assert event.schedule_id == schedule.id
                assert event.finished

    async def test_explicit_cleanup_avoid_schedules_still_having_jobs(
        self, raw_datastore: DataStore
    ) -> None:
        send, receive = create_memory_object_stream[Event](4)
        async with AsyncScheduler(raw_datastore, cleanup_interval=None) as scheduler:
            scheduler.subscribe(send.send, {ScheduleUpdated, JobAdded, JobReleased})
            await scheduler.start_in_background()

            # Add a schedule to immediately set the event
            dummy_event = anyio.Event()
            await scheduler.configure_task("event_set", func=dummy_event.wait)
            schedule_id = await scheduler.add_schedule(
                "event_set", DateTrigger(datetime.now(timezone.utc)), id="event_set"
            )

            # Wait for the job to be submitted
            event = await receive.receive()
            assert isinstance(event, JobAdded)
            assert event.schedule_id == schedule_id

            # Wait for the schedule to be updated
            event = await receive.receive()
            assert isinstance(event, ScheduleUpdated)
            assert event.schedule_id == schedule_id
            assert event.next_fire_time is None

            # Check that there is a job for the schedule
            jobs = await scheduler.get_jobs()
            assert len(jobs) == 1
            assert jobs[0].schedule_id == schedule_id

            # After the cleanup, the schedule should still be around, with a
            # null next_fire_time
            await scheduler.cleanup()
            schedule = await scheduler.get_schedule("event_set")
            assert schedule.next_fire_time is None

            # Wait for the job to finish
            dummy_event.set()
            event = await receive.receive()
            assert isinstance(event, JobReleased)

    async def test_implicit_cleanup(self, mocker: MockerFixture) -> None:
        """
        Test that the scheduler's cleanup() method is called when the scheduler is
        started.

        """
        async with AsyncScheduler() as scheduler:
            event = anyio.Event()
            mocker.patch.object(scheduler.data_store, "cleanup", side_effect=event.set)
            await scheduler.start_in_background()
            with fail_after(3):
                await event.wait()

    async def test_wait_until_stopped(self) -> None:
        async with AsyncScheduler() as scheduler:
            await scheduler.add_job(scheduler.stop)
            await scheduler.wait_until_stopped()

        # This should be a no-op
        await scheduler.wait_until_stopped()


class TestSyncScheduler:
    def test_configure(self) -> None:
        executor = ThreadPoolJobExecutor()
        scheduler = Scheduler(
            identity="identity",
            role=SchedulerRole.scheduler,
            max_concurrent_jobs=150,
            cleanup_interval=5,
            job_executors={"executor1": executor},
            default_job_executor="executor1",
        )
        assert scheduler.identity == "identity"
        assert scheduler.role is SchedulerRole.scheduler
        assert scheduler.max_concurrent_jobs == 150
        assert scheduler.cleanup_interval == timedelta(seconds=5)
        assert scheduler.job_executors == {"executor1": executor}
        assert scheduler.default_job_executor == "executor1"

    @pytest.mark.parametrize("as_default", [False, True])
    def test_threadpool_executor(self, as_default: bool) -> None:
        with Scheduler() as scheduler:
            scheduler.start_in_background()
            if as_default:
                thread_id = scheduler.run_job(threading.get_ident)
            else:
                thread_id = scheduler.run_job(
                    threading.get_ident, job_executor="threadpool"
                )

            assert thread_id != threading.get_ident()

    def test_processpool_executor(self) -> None:
        with Scheduler() as scheduler:
            scheduler.start_in_background()
            pid = scheduler.run_job(os.getpid, job_executor="processpool")
            assert pid != os.getpid()

    def test_properties(self) -> None:
        with Scheduler() as scheduler:
            assert isinstance(scheduler.data_store, MemoryDataStore)
            assert isinstance(scheduler.event_broker, LocalEventBroker)
            assert scheduler.role is SchedulerRole.both
            assert isinstance(scheduler.identity, str)
            assert len(scheduler.job_executors) == 3
            assert isinstance(scheduler.job_executors["async"], AsyncJobExecutor)
            assert isinstance(
                scheduler.job_executors["threadpool"], ThreadPoolJobExecutor
            )
            assert isinstance(
                scheduler.job_executors["processpool"], ProcessPoolJobExecutor
            )
            assert scheduler.default_job_executor == "threadpool"
            assert scheduler.state is RunState.stopped

            scheduler.default_job_executor = "processpool"
            assert scheduler.default_job_executor == "processpool"

    def test_use_without_contextmanager(self, mocker: MockFixture) -> None:
        fake_atexit_register = mocker.patch("atexit.register")
        scheduler = Scheduler()
        scheduler.subscribe(lambda event: None)
        fake_atexit_register.assert_called_once_with(scheduler._exit_stack.close)
        scheduler._exit_stack.close()

    def test_configure_task(self) -> None:
        queue = Queue()
        with Scheduler() as scheduler:
            scheduler.subscribe(queue.put_nowait)
            scheduler.configure_task("mytask", func=dummy_sync_job)
            scheduler.configure_task("mytask", misfire_grace_time=2)
            tasks = scheduler.get_tasks()
            assert len(tasks) == 1
            assert tasks[0].id == "mytask"
            assert tasks[0].func == f"{__name__}:dummy_sync_job"
            assert tasks[0].misfire_grace_time == timedelta(seconds=2)

        event = queue.get(timeout=1)
        assert isinstance(event, TaskAdded)
        assert event.task_id == "mytask"

        event = queue.get(timeout=1)
        assert isinstance(event, TaskUpdated)
        assert event.task_id == "mytask"

    def test_add_remove_schedule(self, timezone: ZoneInfo) -> None:
        queue = Queue()
        with Scheduler() as scheduler:
            scheduler.subscribe(queue.put_nowait)
            now = datetime.now(timezone)
            trigger = DateTrigger(now)
            schedule_id = scheduler.add_schedule(dummy_async_job, trigger, id="foo")
            assert schedule_id == "foo"

            schedules = scheduler.get_schedules()
            assert len(schedules) == 1
            assert schedules[0].id == "foo"
            assert schedules[0].task_id == f"{__name__}:dummy_async_job"

            schedule = scheduler.get_schedule(schedule_id)
            assert schedules[0] == schedule

            scheduler.remove_schedule(schedule_id)
            assert not scheduler.get_schedules()

        event = queue.get(timeout=1)
        assert isinstance(event, TaskAdded)
        assert event.task_id == f"{__name__}:dummy_async_job"

        event = queue.get(timeout=1)
        assert isinstance(event, ScheduleAdded)
        assert event.schedule_id == "foo"
        assert event.next_fire_time == now

        event = queue.get(timeout=1)
        assert isinstance(event, ScheduleRemoved)
        assert event.schedule_id == "foo"
        assert not event.finished

    def test_add_job_wait_result(self) -> None:
        queue = Queue()
        with Scheduler() as scheduler:
            assert scheduler.get_jobs() == []

            scheduler.subscribe(queue.put_nowait)
            job_id = scheduler.add_job(dummy_sync_job, result_expiration_time=10)

            event = queue.get(timeout=1)
            assert isinstance(event, TaskAdded)
            assert event.task_id == f"{__name__}:dummy_sync_job"

            event = queue.get(timeout=1)
            assert isinstance(event, JobAdded)
            assert event.job_id == job_id

            jobs = scheduler.get_jobs()
            assert len(jobs) == 1
            assert jobs[0].id == job_id
            assert jobs[0].task_id == f"{__name__}:dummy_sync_job"

            with pytest.raises(JobLookupError):
                scheduler.get_job_result(job_id, wait=False)

            scheduler.start_in_background()

            event = queue.get(timeout=1)
            assert isinstance(event, SchedulerStarted)

            event = queue.get(timeout=1)
            assert isinstance(event, JobAcquired)
            assert event.job_id == job_id
            assert event.task_id == f"{__name__}:dummy_sync_job"
            assert event.schedule_id is None

            event = queue.get(timeout=1)
            assert isinstance(event, JobReleased)
            assert event.job_id == job_id
            assert event.task_id == f"{__name__}:dummy_sync_job"
            assert event.schedule_id is None
            assert event.outcome is JobOutcome.success

            result = scheduler.get_job_result(job_id)
            assert result.outcome is JobOutcome.success
            assert result.return_value == "returnvalue"

    def test_wait_until_stopped(self) -> None:
        queue = Queue()
        with Scheduler() as scheduler:
            scheduler.configure_task("stop", func=scheduler.stop)
            scheduler.add_job("stop")
            scheduler.subscribe(queue.put_nowait)
            scheduler.start_in_background()
            scheduler.wait_until_stopped()

        event = queue.get(timeout=1)
        assert isinstance(event, SchedulerStarted)

        event = queue.get(timeout=1)
        assert isinstance(event, JobAcquired)

        event = queue.get(timeout=1)
        assert isinstance(event, JobReleased)

        event = queue.get(timeout=1)
        assert isinstance(event, SchedulerStopped)

    def test_explicit_cleanup(self) -> None:
        with Scheduler(cleanup_interval=None) as scheduler:
            event = threading.Event()
            scheduler.add_schedule(
                event.set, DateTrigger(datetime.now(timezone.utc)), id="event_set"
            )
            scheduler.start_in_background()
            assert event.wait(3)

            # The schedule should still be around, but with a null next_fire_time
            schedule = scheduler.get_schedule("event_set")
            assert schedule.next_fire_time is None

            # After the cleanup, the schedule should be gone
            scheduler.cleanup()
            with pytest.raises(ScheduleLookupError):
                scheduler.get_schedule("event_set")

    def test_run_until_stopped(self) -> None:
        queue = Queue()
        with Scheduler() as scheduler:
            scheduler.configure_task("stop", func=scheduler.stop)
            scheduler.add_job("stop")
            scheduler.subscribe(queue.put_nowait)
            scheduler.run_until_stopped()

        event = queue.get(timeout=1)
        assert isinstance(event, SchedulerStarted)

        event = queue.get(timeout=1)
        assert isinstance(event, JobAcquired)

        event = queue.get(timeout=1)
        assert isinstance(event, JobReleased)

        event = queue.get(timeout=1)
        assert isinstance(event, SchedulerStopped)

    def test_uwsgi_threads_error(self, monkeypatch: MonkeyPatch) -> None:
        mod = ModuleType("uwsgi")
        monkeypatch.setitem(sys.modules, "uwsgi", mod)
        mod.has_threads = False
        with pytest.raises(
            RuntimeError, match="The scheduler seems to be running under uWSGI"
        ):
            Scheduler().start_in_background()

    def test_uwsgi_threads_error_subprocess(self) -> None:
        uwsgi_path = Path(sysconfig.get_path("scripts")) / "uwsgi"
        if not uwsgi_path.is_file():
            pytest.skip("uwgsi is not installed")

        # This tests the error with a real uWSGI subprocess
        script_path = (
            Path(__file__).parent.parent / "examples" / "web" / "wsgi_noframework.py"
        )
        assert script_path.is_file()
        proc = subprocess.run(
            ["uwsgi", "--http", ":8000", "--need-app", "--wsgi-file", str(script_path)],
            capture_output=True,
        )
        assert proc.returncode == 22
        assert b"The scheduler seems to be running under uWSGI" in proc.stderr
