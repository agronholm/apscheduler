from __future__ import annotations

import platform
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import datetime, timedelta, timezone
from logging import Logger
from typing import TYPE_CHECKING, AsyncGenerator

import anyio
import pytest
from anyio import CancelScope

from apscheduler import (
    CoalescePolicy,
    ConflictPolicy,
    Event,
    Job,
    JobOutcome,
    JobResult,
    Schedule,
    ScheduleAdded,
    ScheduleRemoved,
    ScheduleUpdated,
    Task,
    TaskAdded,
    TaskLookupError,
    TaskUpdated,
)
from apscheduler.abc import DataStore, EventBroker
from apscheduler.triggers.date import DateTrigger

if TYPE_CHECKING:
    from time_machine import TimeMachineFixture

pytestmark = pytest.mark.anyio


@pytest.fixture
def schedules() -> list[Schedule]:
    trigger = DateTrigger(datetime(2020, 9, 13, tzinfo=timezone.utc))
    schedule1 = Schedule(id="s1", task_id="task1", trigger=trigger)
    schedule1.next_fire_time = trigger.next()

    trigger = DateTrigger(datetime(2020, 9, 14, tzinfo=timezone.utc))
    schedule2 = Schedule(id="s2", task_id="task2", trigger=trigger)
    schedule2.next_fire_time = trigger.next()

    trigger = DateTrigger(datetime(2020, 9, 15, tzinfo=timezone.utc))
    schedule3 = Schedule(id="s3", task_id="task1", trigger=trigger)
    schedule3.next_fire_time = trigger.next()

    return [schedule1, schedule2, schedule3]


@asynccontextmanager
async def capture_events(
    datastore: DataStore,
    limit: int,
    event_types: set[type[Event]] | None = None,
) -> AsyncGenerator[list[Event], None]:
    def listener(event: Event) -> None:
        events.append(event)
        if len(events) == limit:
            limit_event.set()
            subscription.unsubscribe()

    events: list[Event] = []
    limit_event = anyio.Event()
    subscription = datastore._event_broker.subscribe(listener, event_types)
    yield events
    if limit:
        with anyio.fail_after(3):
            await limit_event.wait()


async def test_add_replace_task(datastore: DataStore) -> None:
    event_types = {TaskAdded, TaskUpdated}
    async with capture_events(datastore, 3, event_types) as events:
        await datastore.add_task(
            Task(id="test_task", func="builtins:print", job_executor="async")
        )
        await datastore.add_task(
            Task(id="test_task2", func="math:ceil", job_executor="async")
        )
        await datastore.add_task(
            Task(id="test_task", func="builtins:repr", job_executor="async")
        )

        tasks = await datastore.get_tasks()
        assert len(tasks) == 2
        assert tasks[0].id == "test_task"
        assert tasks[0].func == "builtins:repr"
        assert tasks[1].id == "test_task2"
        assert tasks[1].func == "math:ceil"

    received_event = events.pop(0)
    assert isinstance(received_event, TaskAdded)
    assert received_event.task_id == "test_task"

    received_event = events.pop(0)
    assert isinstance(received_event, TaskAdded)
    assert received_event.task_id == "test_task2"

    received_event = events.pop(0)
    assert isinstance(received_event, TaskUpdated)
    assert received_event.task_id == "test_task"

    assert not events


async def test_add_schedules(datastore: DataStore, schedules: list[Schedule]) -> None:
    async with capture_events(datastore, 3, {ScheduleAdded}) as events:
        for schedule in schedules:
            await datastore.add_schedule(schedule, ConflictPolicy.exception)

        assert await datastore.get_schedules() == schedules
        assert await datastore.get_schedules({"s1", "s2", "s3"}) == schedules
        assert await datastore.get_schedules({"s1"}) == [schedules[0]]
        assert await datastore.get_schedules({"s2"}) == [schedules[1]]
        assert await datastore.get_schedules({"s3"}) == [schedules[2]]

    for event, schedule in zip(events, schedules):
        assert event.schedule_id == schedule.id
        assert event.task_id == schedule.task_id
        assert event.next_fire_time == schedule.next_fire_time


async def test_replace_schedules(
    datastore: DataStore, schedules: list[Schedule]
) -> None:
    async with capture_events(datastore, 1, {ScheduleUpdated}) as events:
        for schedule in schedules:
            await datastore.add_schedule(schedule, ConflictPolicy.exception)

        trigger = DateTrigger(datetime(2020, 9, 16, tzinfo=timezone.utc))
        next_fire_time = trigger.next()
        schedule = Schedule(
            id="s3",
            task_id="foo",
            trigger=trigger,
            args=(),
            kwargs={},
            coalesce=CoalescePolicy.earliest,
            misfire_grace_time=None,
        )
        schedule.next_fire_time = next_fire_time
        await datastore.add_schedule(schedule, ConflictPolicy.replace)

        schedules = await datastore.get_schedules({schedule.id})
        assert schedules[0].task_id == "foo"
        assert schedules[0].next_fire_time == next_fire_time
        assert schedules[0].args == ()
        assert schedules[0].kwargs == {}
        assert schedules[0].coalesce is CoalescePolicy.earliest
        assert schedules[0].misfire_grace_time is None

    received_event = events.pop(0)
    assert received_event.schedule_id == "s3"
    assert received_event.task_id == "foo"
    assert received_event.next_fire_time == datetime(2020, 9, 16, tzinfo=timezone.utc)
    assert not events


async def test_remove_schedules(
    datastore: DataStore, schedules: list[Schedule]
) -> None:
    async with capture_events(datastore, 2, {ScheduleRemoved}) as events:
        for schedule in schedules:
            await datastore.add_schedule(schedule, ConflictPolicy.exception)

        await datastore.remove_schedules(["s1", "s2"])
        assert await datastore.get_schedules() == [schedules[2]]

    received_event = events.pop(0)
    assert received_event.schedule_id == "s1"

    received_event = events.pop(0)
    assert received_event.schedule_id == "s2"

    assert not events


@pytest.mark.skipif(
    platform.python_implementation() != "CPython",
    reason="time-machine is not available",
)
async def test_acquire_release_schedules(
    datastore: DataStore, schedules: list[Schedule], time_machine: TimeMachineFixture
) -> None:
    time_machine.move_to(datetime(2020, 9, 14, tzinfo=timezone.utc))

    event_types = {ScheduleRemoved, ScheduleUpdated}
    async with capture_events(datastore, 2, event_types) as events:
        for schedule in schedules:
            await datastore.add_schedule(schedule, ConflictPolicy.exception)

        # The first scheduler gets the first due schedule
        schedules1 = await datastore.acquire_schedules("dummy-id1", 1)
        assert len(schedules1) == 1
        assert schedules1[0].id == "s1"

        # The second scheduler gets the second due schedule
        schedules2 = await datastore.acquire_schedules("dummy-id2", 1)
        assert len(schedules2) == 1
        assert schedules2[0].id == "s2"

        # The third scheduler gets nothing
        schedules3 = await datastore.acquire_schedules("dummy-id3", 1)
        assert not schedules3

        # Update the schedules and check that the job store actually deletes the
        # first one and updates the second one
        schedules1[0].next_fire_time = None
        schedules2[0].next_fire_time = datetime(2020, 9, 15, tzinfo=timezone.utc)

        # Release all the schedules
        await datastore.release_schedules("dummy-id1", schedules1)
        await datastore.release_schedules("dummy-id2", schedules2)

        # Check that the first schedule has its next fire time nullified
        schedules = await datastore.get_schedules()
        assert len(schedules) == 3
        schedules.sort(key=lambda s: s.id)
        assert schedules[0].id == "s1"
        assert schedules[0].next_fire_time is None
        assert schedules[1].id == "s2"
        assert schedules[2].id == "s3"

    # Check for the appropriate update and delete events
    received_event = events.pop(0)
    assert isinstance(received_event, ScheduleUpdated)
    assert received_event.schedule_id == "s1"
    assert received_event.next_fire_time is None

    received_event = events.pop(0)
    assert isinstance(received_event, ScheduleUpdated)
    assert received_event.schedule_id == "s2"
    assert received_event.next_fire_time == datetime(2020, 9, 15, tzinfo=timezone.utc)

    assert not events


async def test_release_schedule_two_identical_fire_times(datastore: DataStore) -> None:
    """Regression test for #616."""
    for i in range(1, 3):
        trigger = DateTrigger(datetime(2020, 9, 13, tzinfo=timezone.utc))
        schedule = Schedule(id=f"s{i}", task_id="task1", trigger=trigger)
        schedule.next_fire_time = trigger.next()
        await datastore.add_schedule(schedule, ConflictPolicy.exception)

    schedules = await datastore.acquire_schedules("foo", 3)
    schedules[0].next_fire_time = None
    await datastore.release_schedules("foo", schedules)

    remaining = await datastore.get_schedules({s.id for s in schedules})
    assert len(remaining) == 2
    remaining.sort(key=lambda s: s.id)
    assert remaining[0].id == schedules[0].id
    assert remaining[0].next_fire_time is None
    assert remaining[1].id == schedules[1].id
    assert remaining[1].next_fire_time


async def test_release_two_schedules_at_once(datastore: DataStore) -> None:
    """Regression test for #621."""
    for i in range(2):
        trigger = DateTrigger(datetime(2020, 9, 13, tzinfo=timezone.utc))
        schedule = Schedule(id=f"s{i}", task_id="task1", trigger=trigger)
        schedule.next_fire_time = trigger.next()
        await datastore.add_schedule(schedule, ConflictPolicy.exception)

    schedules = await datastore.acquire_schedules("foo", 3)
    await datastore.release_schedules("foo", schedules)

    remaining = await datastore.get_schedules({s.id for s in schedules})
    assert len(remaining) == 2


@pytest.mark.skipif(
    platform.python_implementation() != "CPython",
    reason="time-machine is not available",
)
async def test_acquire_schedules_lock_timeout(
    datastore: DataStore,
    schedules: list[Schedule],
    time_machine: TimeMachineFixture,
) -> None:
    """
    Test that a scheduler can acquire schedules that were acquired by another
    scheduler but not released within the lock timeout period.

    """
    time_machine.move_to(datetime.now(timezone.utc), tick=False)
    await datastore.add_schedule(schedules[0], ConflictPolicy.exception)

    # First, one scheduler acquires the first available schedule
    acquired1 = await datastore.acquire_schedules("dummy-id1", 1)
    assert len(acquired1) == 1
    assert acquired1[0].id == "s1"

    # Try to acquire the schedule just at the threshold (now == acquired_until).
    # This should not yield any schedules.
    time_machine.shift(30)
    acquired2 = await datastore.acquire_schedules("dummy-id2", 1)
    assert not acquired2

    # Right after that, the schedule should be available
    time_machine.shift(1)
    acquired3 = await datastore.acquire_schedules("dummy-id2", 1)
    assert len(acquired3) == 1
    assert acquired3[0].id == "s1"


async def test_acquire_multiple_workers(datastore: DataStore) -> None:
    await datastore.add_task(
        Task(id="task1", func="contextlib:asynccontextmanager", job_executor="async")
    )
    jobs = [Job(task_id="task1") for _ in range(2)]
    for job in jobs:
        await datastore.add_job(job)

    # The first worker gets the first job in the queue
    jobs1 = await datastore.acquire_jobs("worker1", 1)
    assert len(jobs1) == 1
    assert jobs1[0].id == jobs[0].id

    # The second worker gets the second job
    jobs2 = await datastore.acquire_jobs("worker2", 1)
    assert len(jobs2) == 1
    assert jobs2[0].id == jobs[1].id

    # The third worker gets nothing
    jobs3 = await datastore.acquire_jobs("worker3", 1)
    assert not jobs3


async def test_job_release_success(datastore: DataStore) -> None:
    await datastore.add_task(
        Task(id="task1", func="contextlib:asynccontextmanager", job_executor="async")
    )
    job = Job(task_id="task1", result_expiration_time=timedelta(minutes=1))
    await datastore.add_job(job)

    acquired = await datastore.acquire_jobs("worker_id", 2)
    assert len(acquired) == 1
    assert acquired[0].id == job.id

    await datastore.release_job(
        "worker_id",
        acquired[0],
        JobResult.from_job(
            acquired[0],
            JobOutcome.success,
            return_value="foo",
        ),
    )
    result = await datastore.get_job_result(acquired[0].id)
    assert result.outcome is JobOutcome.success
    assert result.exception is None
    assert result.return_value == "foo"

    # Check that the job and its result are gone
    assert not await datastore.get_jobs({acquired[0].id})
    assert not await datastore.get_job_result(acquired[0].id)


async def test_job_release_failure(datastore: DataStore) -> None:
    await datastore.add_task(
        Task(id="task1", job_executor="async", func="contextlib:asynccontextmanager")
    )
    job = Job(task_id="task1", result_expiration_time=timedelta(minutes=1))
    await datastore.add_job(job)

    acquired = await datastore.acquire_jobs("worker_id", 2)
    assert len(acquired) == 1
    assert acquired[0].id == job.id

    await datastore.release_job(
        "worker_id",
        acquired[0],
        JobResult.from_job(
            acquired[0],
            JobOutcome.error,
            exception=ValueError("foo"),
        ),
    )
    result = await datastore.get_job_result(acquired[0].id)
    assert result.outcome is JobOutcome.error
    assert isinstance(result.exception, ValueError)
    assert result.exception.args == ("foo",)
    assert result.return_value is None

    # Check that the job and its result are gone
    assert not await datastore.get_jobs({acquired[0].id})
    assert not await datastore.get_job_result(acquired[0].id)


async def test_job_release_missed_deadline(datastore: DataStore):
    await datastore.add_task(
        Task(id="task1", func="contextlib:asynccontextmanager", job_executor="async")
    )
    job = Job(task_id="task1", result_expiration_time=timedelta(minutes=1))
    await datastore.add_job(job)

    acquired = await datastore.acquire_jobs("worker_id", 2)
    assert len(acquired) == 1
    assert acquired[0].id == job.id

    await datastore.release_job(
        "worker_id",
        acquired[0],
        JobResult.from_job(
            acquired[0],
            JobOutcome.missed_start_deadline,
        ),
    )
    result = await datastore.get_job_result(acquired[0].id)
    assert result.outcome is JobOutcome.missed_start_deadline
    assert result.exception is None
    assert result.return_value is None

    # Check that the job and its result are gone
    assert not await datastore.get_jobs({acquired[0].id})
    assert not await datastore.get_job_result(acquired[0].id)


async def test_job_release_cancelled(datastore: DataStore) -> None:
    await datastore.add_task(
        Task(id="task1", func="contextlib:asynccontextmanager", job_executor="async")
    )
    job = Job(task_id="task1", result_expiration_time=timedelta(minutes=1))
    await datastore.add_job(job)

    acquired = await datastore.acquire_jobs("worker1", 2)
    assert len(acquired) == 1
    assert acquired[0].id == job.id

    await datastore.release_job(
        "worker1",
        acquired[0],
        JobResult.from_job(acquired[0], JobOutcome.cancelled),
    )
    result = await datastore.get_job_result(acquired[0].id)
    assert result.outcome is JobOutcome.cancelled
    assert result.exception is None
    assert result.return_value is None

    # Check that the job and its result are gone
    assert not await datastore.get_jobs({acquired[0].id})
    assert not await datastore.get_job_result(acquired[0].id)


@pytest.mark.skipif(
    platform.python_implementation() != "CPython",
    reason="time-machine is not available",
)
async def test_acquire_jobs_lock_timeout(
    datastore: DataStore, time_machine: TimeMachineFixture
) -> None:
    """
    Test that a worker can acquire jobs that were acquired by another scheduler but
    not released within the lock timeout period.

    """
    await datastore.add_task(
        Task(id="task1", func="contextlib:asynccontextmanager", job_executor="async")
    )
    job = Job(task_id="task1", result_expiration_time=timedelta(minutes=1))
    await datastore.add_job(job)

    # First, one worker acquires the first available job
    time_machine.move_to(datetime.now(timezone.utc), tick=False)
    acquired = await datastore.acquire_jobs("worker1", 1)
    assert len(acquired) == 1
    assert acquired[0].id == job.id

    # Try to acquire the job just at the threshold (now == acquired_until).
    # This should not yield any jobs.
    time_machine.shift(30)
    assert not await datastore.acquire_jobs("worker2", 1)

    # Right after that, the job should be available
    time_machine.shift(1)
    acquired = await datastore.acquire_jobs("worker2", 1)
    assert len(acquired) == 1
    assert acquired[0].id == job.id


async def test_acquire_jobs_max_number_exceeded(datastore: DataStore) -> None:
    await datastore.add_task(
        Task(
            id="task1",
            func="contextlib:asynccontextmanager",
            job_executor="async",
            max_running_jobs=2,
        )
    )
    jobs = [Job(task_id="task1"), Job(task_id="task1"), Job(task_id="task1")]
    for job in jobs:
        await datastore.add_job(job)

    # Check that only 2 jobs are returned from acquire_jobs() even though the limit
    # wqas 3
    acquired_jobs = await datastore.acquire_jobs("worker1", 3)
    assert [job.id for job in acquired_jobs] == [job.id for job in jobs[:2]]

    # Release one job, and the worker should be able to acquire the third job
    await datastore.release_job(
        "worker1",
        acquired_jobs[0],
        JobResult.from_job(
            acquired_jobs[0],
            JobOutcome.success,
            return_value=None,
        ),
    )
    acquired_jobs = await datastore.acquire_jobs("worker1", 3)
    assert [job.id for job in acquired_jobs] == [jobs[2].id]


async def test_add_get_task(datastore: DataStore) -> None:
    with pytest.raises(TaskLookupError):
        await datastore.get_task("dummyid")

    await datastore.add_task(
        Task(id="dummyid", func="contextlib:asynccontextmanager", job_executor="async")
    )
    task = await datastore.get_task("dummyid")
    assert task.id == "dummyid"
    assert task.func == "contextlib:asynccontextmanager"


async def test_cancel_start(
    raw_datastore: DataStore, local_broker: EventBroker, logger: Logger
) -> None:
    with CancelScope() as scope:
        scope.cancel()
        async with AsyncExitStack() as exit_stack:
            await raw_datastore.start(exit_stack, local_broker, logger)


async def test_cancel_stop(
    raw_datastore: DataStore, local_broker: EventBroker, logger: Logger
) -> None:
    with CancelScope() as scope:
        async with AsyncExitStack() as exit_stack:
            await raw_datastore.start(exit_stack, local_broker, logger)
            scope.cancel()


async def test_next_schedule_run_time(datastore: DataStore, schedules: list[Schedule]):
    next_schedule_run_time = await datastore.get_next_schedule_run_time()
    assert next_schedule_run_time is None

    for schedule in schedules:
        await datastore.add_schedule(schedule, ConflictPolicy.exception)

    next_schedule_run_time = await datastore.get_next_schedule_run_time()
    assert next_schedule_run_time == datetime(2020, 9, 13, tzinfo=timezone.utc)
