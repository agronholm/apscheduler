from __future__ import annotations

import os
import subprocess
import sys
import sysconfig
import threading
import time
from collections import defaultdict
from collections.abc import Callable
from contextlib import AsyncExitStack
from datetime import datetime, timedelta, timezone
from functools import partial
from inspect import signature
from pathlib import Path
from queue import Queue
from types import ModuleType
from typing import Any, cast
from unittest.mock import patch

import anyio
import pytest
from anyio import (
    Lock,
    WouldBlock,
    create_memory_object_stream,
    fail_after,
    move_on_after,
    sleep,
)
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
    TaskDefaults,
    TaskUpdated,
    current_async_scheduler,
    current_job,
    task,
)
from apscheduler.abc import DataStore
from apscheduler.datastores.base import BaseExternalDataStore
from apscheduler.datastores.memory import MemoryDataStore
from apscheduler.eventbrokers.local import LocalEventBroker
from apscheduler.executors.async_ import AsyncJobExecutor
from apscheduler.executors.subprocess import ProcessPoolJobExecutor
from apscheduler.executors.thread import ThreadPoolJobExecutor
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

if sys.version_info >= (3, 11):
    from datetime import UTC
else:
    UTC = timezone.utc
    from exceptiongroup import ExceptionGroup

from zoneinfo import ZoneInfo

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


@task(
    job_executor="threadpool",
    max_running_jobs=3,
    misfire_grace_time=timedelta(seconds=6),
)
def decorated_job() -> None:
    pass


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
    def test_repr(self) -> None:
        scheduler = AsyncScheduler(identity="my identity")
        assert repr(scheduler) == (
            "AsyncScheduler(identity='my identity', role=<SchedulerRole.both: 3>, "
            "data_store=MemoryDataStore(), event_broker=LocalEventBroker())"
        )

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
            assert scheduler.task_defaults.job_executor == "async"
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
        with send, receive:
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

    async def test_configure_task_with_decorator(self) -> None:
        async with AsyncScheduler() as scheduler:
            await scheduler.configure_task("taskfunc", func=decorated_job)
            tasks = await scheduler.get_tasks()
            assert len(tasks) == 1
            assert tasks[0].max_running_jobs == 3
            assert tasks[0].misfire_grace_time == timedelta(seconds=6)
            assert tasks[0].job_executor == "threadpool"

    async def test_configure_local_task_with_decorator(self) -> None:
        @task(
            id="taskfunc",
            job_executor="threadpool",
            max_running_jobs=3,
            misfire_grace_time=timedelta(seconds=6),
            metadata={"local": 6},
        )
        def taskfunc() -> None:
            pass

        task_defaults = TaskDefaults(metadata={"global": "foo"})
        async with AsyncScheduler(task_defaults=task_defaults) as scheduler:
            await scheduler.configure_task(taskfunc, metadata={"direct": [1, 9]})
            tasks = await scheduler.get_tasks()
            assert len(tasks) == 1
            assert tasks[0].id == "taskfunc"
            assert tasks[0].max_running_jobs == 3
            assert tasks[0].misfire_grace_time == timedelta(seconds=6)
            assert tasks[0].job_executor == "threadpool"
            assert tasks[0].metadata == {"global": "foo", "local": 6, "direct": [1, 9]}

    async def test_add_pause_unpause_remove_schedule(
        self, raw_datastore: DataStore, timezone: ZoneInfo
    ) -> None:
        send, receive = create_memory_object_stream[Event](5)
        with send, receive:
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
        async with AsyncExitStack() as exit_stack:
            exit_stack.enter_context(send)
            exit_stack.enter_context(receive)
            scheduler = await exit_stack.enter_async_context(
                AsyncScheduler(data_store=raw_datastore)
            )
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
                assert event.scheduled_start is None
                acquired_at = event.timestamp

                event = await receive.receive()
                assert isinstance(event, JobReleased)
                assert event.job_id == job_id
                assert event.task_id == f"{__name__}:dummy_async_job"
                assert event.schedule_id is None
                assert event.scheduled_start is None
                assert event.started_at is not None
                assert event.started_at >= acquired_at
                assert event.outcome is JobOutcome.success

                result = await scheduler.get_job_result(job_id)
                assert result
                assert result.outcome is JobOutcome.success
                assert result.return_value == "returnvalue"

    @pytest.mark.parametrize("success", [True, False])
    async def test_run_job(self, raw_datastore: DataStore, success: bool) -> None:
        send, receive = create_memory_object_stream[Event](4)
        with send, receive:
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
                    assert event.scheduled_start is None
                    assert event.scheduler_id == scheduler.identity
                    acquired_at = event.timestamp

                    # The scheduler released the job
                    event = await receive.receive()
                    assert isinstance(event, JobReleased)
                    assert event.job_id == job_id
                    assert event.task_id == f"{__name__}:dummy_async_job"
                    assert event.schedule_id is None
                    assert event.scheduled_start is None
                    assert event.started_at is not None
                    assert event.started_at >= acquired_at
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
        target: Callable[..., Any],
        expected_result: object,
        use_scheduling: bool,
        raw_datastore: DataStore,
        timezone: ZoneInfo,
    ) -> None:
        now = datetime.now(timezone)
        send, receive = create_memory_object_stream[Event](4)
        async with AsyncExitStack() as exit_stack:
            exit_stack.enter_context(send)
            exit_stack.enter_context(receive)
            scheduler = await exit_stack.enter_async_context(
                AsyncScheduler(data_store=raw_datastore)
            )
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
                assert result
                assert result.outcome is JobOutcome.success
                assert result.return_value == expected_result

    async def test_scheduled_job_missed_deadline(
        self, raw_datastore: DataStore, timezone: ZoneInfo
    ) -> None:
        one_second_in_past = datetime.now(timezone) - timedelta(seconds=1)
        trigger = DateTrigger(one_second_in_past)
        scheduler_send, scheduler_receive = create_memory_object_stream[Event](4)
        worker_send, worker_receive = create_memory_object_stream[Event](2)
        with scheduler_send, scheduler_receive, worker_send, worker_receive:
            async with AsyncExitStack() as exit_stack:
                scheduler = await exit_stack.enter_async_context(
                    AsyncScheduler(data_store=raw_datastore)
                )
                await scheduler.add_schedule(
                    dummy_async_job, trigger, misfire_grace_time=0, id="foo"
                )
                await scheduler.start_in_background()
                exit_stack.enter_context(
                    scheduler.subscribe(
                        scheduler_send.send, {JobAdded, ScheduleUpdated}
                    )
                )
                exit_stack.enter_context(
                    scheduler.subscribe(worker_send.send, {JobAcquired, JobReleased})
                )
                with fail_after(3):
                    # The schedule was processed and a job was added for it
                    event = await scheduler_receive.receive()
                    assert isinstance(event, JobAdded)
                    assert event.schedule_id == "foo"
                    assert event.task_id == "test_schedulers:dummy_async_job"
                    job_id = event.job_id

                    # The schedule was updated with a null next fire time
                    event = await scheduler_receive.receive()
                    assert isinstance(event, ScheduleUpdated)
                    assert event.schedule_id == "foo"
                    assert event.next_fire_time is None

                    # The new job was acquired
                    event = await worker_receive.receive()
                    assert isinstance(event, JobReleased)
                    assert event.job_id == job_id
                    assert event.task_id == "test_schedulers:dummy_async_job"
                    assert event.schedule_id == "foo"
                    assert event.scheduled_start == one_second_in_past
                    assert event.started_at is None
                    assert event.outcome is JobOutcome.missed_start_deadline

            # There should be no more events on the list
            with pytest.raises(WouldBlock):
                scheduler_receive.receive_nowait()

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
        now = datetime.now(timezone)
        first_start_time = now - timedelta(minutes=3, seconds=5)
        trigger = IntervalTrigger(minutes=1, start_time=first_start_time)
        send, receive = create_memory_object_stream[Event](4)
        with send, receive:
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
        jitter = 1.569374
        now = datetime.now(timezone)
        fake_uniform = mocker.patch("random.uniform")
        fake_uniform.configure_mock(side_effect=lambda a, b: jitter)
        send, receive = create_memory_object_stream[Event](4)
        async with AsyncExitStack() as exit_stack:
            exit_stack.enter_context(send)
            exit_stack.enter_context(receive)
            scheduler = await exit_stack.enter_async_context(
                AsyncScheduler(data_store=raw_datastore, role=SchedulerRole.scheduler)
            )
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

            assert result
            assert result.job_id == job_id
            assert result.outcome is JobOutcome.success
            assert result.return_value == "returnvalue"

    async def test_add_job_get_result_empty(self, raw_datastore: DataStore) -> None:
        send, receive = create_memory_object_stream[Event](4)
        async with AsyncExitStack() as exit_stack:
            exit_stack.enter_context(send)
            exit_stack.enter_context(receive)
            scheduler = await exit_stack.enter_async_context(
                AsyncScheduler(data_store=raw_datastore)
            )
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

            assert result
            assert result.job_id == job_id
            assert result.outcome is JobOutcome.error
            assert isinstance(result.exception, RuntimeError)
            assert str(result.exception) == "failing as requested"

    async def test_add_job_get_result_no_ready_yet(self) -> None:
        send, receive = create_memory_object_stream[Event](4)
        async with AsyncExitStack() as exit_stack:
            exit_stack.enter_context(send)
            exit_stack.enter_context(receive)
            scheduler = await exit_stack.enter_async_context(AsyncScheduler())
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

    async def test_add_job_not_rewriting_task_config(
        self, raw_datastore: DataStore
    ) -> None:
        async with AsyncScheduler(data_store=raw_datastore) as scheduler:
            TASK_ID = "task_dummy_async_job"
            JOB_EXECUTOR = "async"
            MISFIRE_GRACE_TIME = timedelta(seconds=10)
            MAX_RUNNING_JOBS = 5
            METADATA = {"key": "value"}

            await scheduler.configure_task(
                func_or_task_id=TASK_ID,
                func=dummy_async_job,
                job_executor=JOB_EXECUTOR,
                misfire_grace_time=MISFIRE_GRACE_TIME,
                max_running_jobs=MAX_RUNNING_JOBS,
                metadata=METADATA,
            )

            assert await scheduler.add_job(TASK_ID)

            task = await scheduler.data_store.get_task(TASK_ID)
            assert task.job_executor == JOB_EXECUTOR
            assert task.misfire_grace_time == MISFIRE_GRACE_TIME
            assert task.max_running_jobs == MAX_RUNNING_JOBS
            assert task.metadata == METADATA

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
        now = datetime.now(timezone)
        send, receive = create_memory_object_stream[Event](1)
        async with AsyncExitStack() as exit_stack:
            exit_stack.enter_context(send)
            exit_stack.enter_context(receive)
            scheduler = await exit_stack.enter_async_context(AsyncScheduler())
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
        async with AsyncExitStack() as exit_stack:
            exit_stack.enter_context(send)
            exit_stack.enter_context(receive)
            scheduler = await exit_stack.enter_async_context(
                AsyncScheduler(raw_datastore, cleanup_interval=None)
            )
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
            await sleep(0.1)
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
        async with AsyncExitStack() as exit_stack:
            exit_stack.enter_context(send)
            exit_stack.enter_context(receive)
            scheduler = await exit_stack.enter_async_context(
                AsyncScheduler(raw_datastore, cleanup_interval=None)
            )
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

    async def test_max_concurrent_jobs(self) -> None:
        lock = Lock()
        scheduler = AsyncScheduler(max_concurrent_jobs=1)
        tasks_done = 0

        async def acquire_release() -> None:
            nonlocal tasks_done
            lock.acquire_nowait()
            await sleep(0.1)
            tasks_done += 1
            if tasks_done == 2:
                await scheduler.stop()

            lock.release()

        with fail_after(3):
            async with scheduler:
                await scheduler.configure_task("dummyjob", func=acquire_release)
                await scheduler.add_job("dummyjob")
                await scheduler.add_job("dummyjob")
                await scheduler.run_until_stopped()

    @pytest.mark.parametrize(
        "trigger_type, run_job",
        [
            pytest.param("cron", False, id="cron"),
            pytest.param("date", True, id="date"),
        ],
    )
    async def test_pause_unpause_schedule(
        self,
        raw_datastore: DataStore,
        timezone: ZoneInfo,
        trigger_type: str,
        run_job: bool,
    ) -> None:
        if trigger_type == "cron":
            trigger = CronTrigger()
        else:
            trigger = DateTrigger(datetime.now(timezone))

        async with AsyncExitStack() as exit_stack:
            send, receive = create_memory_object_stream[Event](4)
            exit_stack.enter_context(send)
            exit_stack.enter_context(receive)
            scheduler = await exit_stack.enter_async_context(
                AsyncScheduler(data_store=raw_datastore, role=SchedulerRole.scheduler)
            )
            schedule_id = await scheduler.add_schedule(
                dummy_async_job, trigger, id="foo"
            )
            scheduler.subscribe(send.send, {ScheduleUpdated, JobAdded})

            # Pause the schedule and wait for the schedule update event
            await scheduler.pause_schedule(schedule_id)
            schedule = await scheduler.get_schedule(schedule_id)
            assert schedule.paused
            event = await receive.receive()
            assert isinstance(event, ScheduleUpdated)

            if run_job:
                # Make sure that no jobs are added when the scheduler is started
                await scheduler.start_in_background()
                assert not await scheduler.get_jobs()

            # Unpause the schedule and wait for the schedule update event
            await scheduler.unpause_schedule(schedule_id)
            schedule = await scheduler.get_schedule(schedule_id)
            assert not schedule.paused
            event = await receive.receive()
            assert isinstance(event, ScheduleUpdated)

            if run_job:
                with fail_after(3):
                    job_added_event = await receive.receive()

                assert isinstance(job_added_event, JobAdded)
                assert job_added_event.schedule_id == schedule_id

    async def test_schedule_job_result_expiration_time(
        self, raw_datastore: DataStore, timezone: ZoneInfo
    ) -> None:
        trigger = DateTrigger(datetime.now(timezone))
        send, receive = create_memory_object_stream[Event](4)
        with send, receive:
            async with AsyncExitStack() as exit_stack:
                scheduler = await exit_stack.enter_async_context(
                    AsyncScheduler(data_store=raw_datastore)
                )
                await scheduler.add_schedule(
                    dummy_async_job, trigger, id="foo", job_result_expiration_time=10
                )
                exit_stack.enter_context(scheduler.subscribe(send.send, {JobAdded}))
                await scheduler.start_in_background()

                # Wait for the scheduled job to be added
                with fail_after(3):
                    event = await receive.receive()
                    assert isinstance(event, JobAdded)
                    assert event.schedule_id == "foo"

                    # Get its result
                    result = await scheduler.get_job_result(event.job_id)

                assert result
                assert result.outcome is JobOutcome.success
                assert result.return_value == "returnvalue"

    async def test_scheduler_crash_restart_schedule_immediately(
        self, raw_datastore: DataStore, timezone: ZoneInfo
    ) -> None:
        """
        Test that the scheduler can immediately start processing a schedule it had
        acquired while the crash occurred.

        """
        scheduler = AsyncScheduler(data_store=raw_datastore)
        error_patch = patch.object(
            raw_datastore, "release_schedules", side_effect=RuntimeError("Fake failure")
        )
        with pytest.raises(ExceptionGroup) as exc_info, error_patch:
            async with scheduler:
                await scheduler.add_schedule(
                    dummy_async_job, IntervalTrigger(minutes=1), id="foo"
                )
                with move_on_after(3):
                    await scheduler.run_until_stopped()

                pytest.fail("The scheduler did not crash")

        exc = exc_info.value
        while isinstance(exc, ExceptionGroup) and len(exc.exceptions) == 1:
            exc = exc.exceptions[0]

        assert isinstance(exc, RuntimeError)
        assert exc.args == ("Fake failure",)

        # Don't clear the data store at launch
        if isinstance(raw_datastore, BaseExternalDataStore):
            raw_datastore.start_from_scratch = False

        # Now reinitialize the scheduler and make sure the schedule gets processed
        # immediately
        async with scheduler:
            # Check that the schedule was left in an acquired state
            schedules = await scheduler.get_schedules()
            assert len(schedules) == 1
            assert schedules[0].acquired_by == scheduler.identity
            assert schedules[0].acquired_until > datetime.now(timezone)

            # Start the scheduler and wait for the schedule to be processed
            await scheduler.start_in_background()
            with fail_after(scheduler.lease_duration.total_seconds() / 2):
                job_added_event = await scheduler.get_next_event(JobAdded)

            assert job_added_event.schedule_id == "foo"

    async def test_scheduler_crash_reap_abandoned_jobs(
        self, raw_datastore: DataStore, timezone: ZoneInfo
    ) -> None:
        """
        Test that after the scheduler has crashed and been restarted, it immediately
        detects an abandoned job and releases it with the appropriate result code.

        """
        scheduler = AsyncScheduler(data_store=raw_datastore)
        error_patch = patch.object(
            raw_datastore, "release_job", side_effect=RuntimeError("Fake failure")
        )
        with pytest.raises(ExceptionGroup) as exc_info, error_patch:
            async with scheduler:
                job_id = await scheduler.add_job(dummy_async_job)
                with move_on_after(3):
                    await scheduler.run_until_stopped()

                pytest.fail("The scheduler did not crash")

        exc = exc_info.value
        while isinstance(exc, ExceptionGroup) and len(exc.exceptions) == 1:
            exc = exc.exceptions[0]

        assert isinstance(exc, RuntimeError)
        assert exc.args == ("Fake failure",)

        # Don't clear the data store at launch
        if isinstance(raw_datastore, BaseExternalDataStore):
            raw_datastore.start_from_scratch = False

        # Now reinitialize the scheduler and make sure the job gets processed
        # immediately
        async with scheduler:
            # Check that the job was left in an acquired state
            jobs = await scheduler.get_jobs()
            assert len(jobs) == 1
            assert jobs[0].acquired_by == scheduler.identity
            assert jobs[0].acquired_until > datetime.now(timezone)

            trigger_event = anyio.Event()
            job_released_event: JobReleased | None = None

            def event_callback(event: Event) -> None:
                nonlocal job_released_event
                job_released_event = cast(JobReleased, event)
                trigger_event.set()

            # Start the scheduler and wait for the job to be processed
            with scheduler.subscribe(event_callback, {JobReleased}):
                await scheduler.start_in_background()
                with fail_after(scheduler.lease_duration.total_seconds() / 2):
                    await trigger_event.wait()

            assert job_released_event
            assert job_released_event.job_id == job_id
            assert job_released_event.outcome is JobOutcome.abandoned
            assert not await scheduler.get_jobs()


class TestSyncScheduler:
    def test_interface_parity(self) -> None:
        """
        Ensure that the sync scheduler has the same properties and methods as the async
        schedulers, and the method parameters match too.

        """
        actual_attributes = set(dir(Scheduler))
        expected_attributes = sorted(
            attrname for attrname in dir(AsyncScheduler) if not attrname.startswith("_")
        )
        for attrname in expected_attributes:
            if attrname not in actual_attributes:
                pytest.fail(f"SyncScheduler is missing the {attrname} attribute")

            async_attrval = getattr(AsyncScheduler, attrname)
            sync_attrval = getattr(Scheduler, attrname)
            if callable(async_attrval):
                async_sig = signature(async_attrval)
                async_args: dict[int, list] = defaultdict(list)
                for param in async_sig.parameters.values():
                    if param.name not in ("task_status", "is_async"):
                        async_args[param.kind].append(param)

                sync_sig = signature(sync_attrval)
                sync_args: dict[int, list] = defaultdict(list)
                for param in sync_sig.parameters.values():
                    sync_args[param.kind].append(param)

                for kind, args in async_args.items():
                    assert args == sync_args[kind], (
                        f"Parameter mismatch for {attrname}(): {args} != {sync_args[kind]}"
                    )

    def test_repr(self) -> None:
        scheduler = Scheduler(identity="my identity")
        assert repr(scheduler) == (
            "Scheduler(identity='my identity', role=<SchedulerRole.both: 3>, "
            "data_store=MemoryDataStore(), event_broker=LocalEventBroker())"
        )

    def test_configure(self) -> None:
        executor = ThreadPoolJobExecutor()
        task_defaults = TaskDefaults(job_executor="executor1")
        scheduler = Scheduler(
            identity="identity",
            role=SchedulerRole.scheduler,
            max_concurrent_jobs=150,
            cleanup_interval=5,
            job_executors={"executor1": executor},
            task_defaults=task_defaults,
        )
        assert scheduler.identity == "identity"
        assert scheduler.role is SchedulerRole.scheduler
        assert scheduler.max_concurrent_jobs == 150
        assert scheduler.cleanup_interval == timedelta(seconds=5)
        assert scheduler.job_executors == {"executor1": executor}
        assert scheduler.task_defaults == task_defaults

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
            assert isinstance(scheduler.task_defaults, TaskDefaults)
            assert scheduler.state is RunState.stopped

    def test_use_without_contextmanager(self, mocker: MockFixture) -> None:
        fake_atexit_register = mocker.patch("atexit.register")
        scheduler = Scheduler()
        scheduler.subscribe(lambda event: None)
        fake_atexit_register.assert_called_once_with(scheduler._exit_stack.close)
        scheduler._exit_stack.close()

    def test_configure_task(self) -> None:
        queue: Queue[Event] = Queue()
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
        queue: Queue[Event] = Queue()
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
        queue: Queue[Event] = Queue()
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
            assert result
            assert result.outcome is JobOutcome.success
            assert result.return_value == "returnvalue"

    def test_wait_until_stopped(self) -> None:
        queue: Queue[Event] = Queue()
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
        queue: Queue[Event] = Queue()
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
            pytest.skip("uwsgi is not installed")

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
