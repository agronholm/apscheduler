from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import AsyncContextManager, AsyncGenerator, List, Optional, Set, Type

import anyio
import pytest
from freezegun.api import FrozenDateTimeFactory

from apscheduler.abc import AsyncDataStore, Job, Schedule
from apscheduler.enums import CoalescePolicy, ConflictPolicy, JobOutcome
from apscheduler.events import Event, ScheduleAdded, ScheduleRemoved, ScheduleUpdated, TaskAdded
from apscheduler.structures import JobResult, Task
from apscheduler.triggers.date import DateTrigger


@pytest.fixture
def schedules() -> List[Schedule]:
    trigger = DateTrigger(datetime(2020, 9, 13, tzinfo=timezone.utc))
    schedule1 = Schedule(id='s1', task_id='task1', trigger=trigger)
    schedule1.next_fire_time = trigger.next()

    trigger = DateTrigger(datetime(2020, 9, 14, tzinfo=timezone.utc))
    schedule2 = Schedule(id='s2', task_id='task2', trigger=trigger)
    schedule2.next_fire_time = trigger.next()

    trigger = DateTrigger(datetime(2020, 9, 15, tzinfo=timezone.utc))
    schedule3 = Schedule(id='s3', task_id='task1', trigger=trigger)
    return [schedule1, schedule2, schedule3]


@asynccontextmanager
async def capture_events(
    store: AsyncDataStore, limit: int,
    event_types: Optional[Set[Type[Event]]] = None
) -> AsyncGenerator[List[Event], None, None]:
    def listener(event: Event) -> None:
        events.append(event)
        if len(events) == limit:
            limit_event.set()
            store.unsubscribe(token)

    events: List[Event] = []
    limit_event = anyio.Event()
    token = store.subscribe(listener, event_types)
    yield events
    if limit:
        with anyio.fail_after(3):
            await limit_event.wait()


@pytest.mark.anyio
class TestAsyncStores:
    async def test_add_replace_task(
            self, datastore_cm: AsyncContextManager[AsyncDataStore]) -> None:
        import math

        async with datastore_cm as store, capture_events(store, 3, {TaskAdded}) as events:
            await store.add_task(Task(id='test_task', func=print))
            await store.add_task(Task(id='test_task2', func=math.ceil))
            await store.add_task(Task(id='test_task', func=repr))

            tasks = await store.get_tasks()
            assert len(tasks) == 2
            assert tasks[0].id == 'test_task'
            assert tasks[0].func is repr
            assert tasks[1].id == 'test_task2'
            assert tasks[1].func is math.ceil

        received_event = events.pop(0)
        assert received_event.task_id == 'test_task'

        received_event = events.pop(0)
        assert received_event.task_id == 'test_task2'

        received_event = events.pop(0)
        assert received_event.task_id == 'test_task'

        assert not events

    async def test_add_schedules(self, datastore_cm: AsyncContextManager[AsyncDataStore],
                                 schedules: List[Schedule]) -> None:
        async with datastore_cm as store, capture_events(store, 3, {ScheduleAdded}) as events:
            for schedule in schedules:
                await store.add_schedule(schedule, ConflictPolicy.exception)

            assert await store.get_schedules() == schedules
            assert await store.get_schedules({'s1', 's2', 's3'}) == schedules
            assert await store.get_schedules({'s1'}) == [schedules[0]]
            assert await store.get_schedules({'s2'}) == [schedules[1]]
            assert await store.get_schedules({'s3'}) == [schedules[2]]

        for event, schedule in zip(events, schedules):
            assert event.schedule_id == schedule.id
            assert event.next_fire_time == schedule.next_fire_time

    async def test_replace_schedules(self, datastore_cm: AsyncContextManager[AsyncDataStore],
                                     schedules: List[Schedule]) -> None:
        async with datastore_cm as store, capture_events(store, 1, {ScheduleUpdated}) as events:
            for schedule in schedules:
                await store.add_schedule(schedule, ConflictPolicy.exception)

            next_fire_time = schedules[2].trigger.next()
            schedule = Schedule(id='s3', task_id='foo', trigger=schedules[2].trigger, args=(),
                                kwargs={}, coalesce=CoalescePolicy.earliest,
                                misfire_grace_time=None, tags=frozenset())
            schedule.next_fire_time = next_fire_time
            await store.add_schedule(schedule, ConflictPolicy.replace)

            schedules = await store.get_schedules({schedule.id})
            assert schedules[0].task_id == 'foo'
            assert schedules[0].next_fire_time == next_fire_time
            assert schedules[0].args == ()
            assert schedules[0].kwargs == {}
            assert schedules[0].coalesce is CoalescePolicy.earliest
            assert schedules[0].misfire_grace_time is None
            assert schedules[0].tags == frozenset()

        received_event = events.pop(0)
        assert received_event.schedule_id == 's3'
        assert received_event.next_fire_time == datetime(2020, 9, 15, tzinfo=timezone.utc)
        assert not events

    async def test_remove_schedules(self, datastore_cm: AsyncContextManager[AsyncDataStore],
                                    schedules: List[Schedule]) -> None:
        async with datastore_cm as store, capture_events(store, 2, {ScheduleRemoved}) as events:
            for schedule in schedules:
                await store.add_schedule(schedule, ConflictPolicy.exception)

            await store.remove_schedules(['s1', 's2'])
            assert await store.get_schedules() == [schedules[2]]

        received_event = events.pop(0)
        assert received_event.schedule_id == 's1'

        received_event = events.pop(0)
        assert received_event.schedule_id == 's2'

        assert not events

    @pytest.mark.freeze_time(datetime(2020, 9, 14, tzinfo=timezone.utc))
    async def test_acquire_release_schedules(
            self, datastore_cm, schedules: List[Schedule]) -> None:
        event_types = {ScheduleRemoved, ScheduleUpdated}
        async with datastore_cm as store, capture_events(store, 2, event_types) as events:
            for schedule in schedules:
                await store.add_schedule(schedule, ConflictPolicy.exception)

            # The first scheduler gets the first due schedule
            schedules1 = await store.acquire_schedules('dummy-id1', 1)
            assert len(schedules1) == 1
            assert schedules1[0].id == 's1'

            # The second scheduler gets the second due schedule
            schedules2 = await store.acquire_schedules('dummy-id2', 1)
            assert len(schedules2) == 1
            assert schedules2[0].id == 's2'

            # The third scheduler gets nothing
            schedules3 = await store.acquire_schedules('dummy-id3', 1)
            assert not schedules3

            # Update the schedules and check that the job store actually deletes the first
            # one and updates the second one
            schedules1[0].next_fire_time = None
            schedules2[0].next_fire_time = datetime(2020, 9, 15, tzinfo=timezone.utc)

            # Release all the schedules
            await store.release_schedules('dummy-id1', schedules1)
            await store.release_schedules('dummy-id2', schedules2)

            # Check that the first schedule is gone
            schedules = await store.get_schedules()
            assert len(schedules) == 2
            assert schedules[0].id == 's2'
            assert schedules[1].id == 's3'

        # Check for the appropriate update and delete events
        received_event = events.pop(0)
        assert isinstance(received_event, ScheduleRemoved)
        assert received_event.schedule_id == 's1'

        received_event = events.pop(0)
        assert isinstance(received_event, ScheduleUpdated)
        assert received_event.schedule_id == 's2'
        assert received_event.next_fire_time == datetime(2020, 9, 15, tzinfo=timezone.utc)

        assert not events

    async def test_acquire_schedules_lock_timeout(
            self, datastore_cm, schedules: List[Schedule], freezer) -> None:
        """
        Test that a scheduler can acquire schedules that were acquired by another scheduler but
        not released within the lock timeout period.

        """
        async with datastore_cm as store:
            await store.add_schedule(schedules[0], ConflictPolicy.exception)

            # First, one scheduler acquires the first available schedule
            acquired1 = await store.acquire_schedules('dummy-id1', 1)
            assert len(acquired1) == 1
            assert acquired1[0].id == 's1'

            # Try to acquire the schedule just at the threshold (now == acquired_until).
            # This should not yield any schedules.
            freezer.tick(30)
            acquired2 = await store.acquire_schedules('dummy-id2', 1)
            assert not acquired2

            # Right after that, the schedule should be available
            freezer.tick(1)
            acquired3 = await store.acquire_schedules('dummy-id2', 1)
            assert len(acquired3) == 1
            assert acquired3[0].id == 's1'

    async def test_acquire_multiple_workers(
            self, datastore_cm: AsyncContextManager[AsyncDataStore]) -> None:
        async with datastore_cm as store:
            await store.add_task(Task(id='task1', func=asynccontextmanager))
            jobs = [Job(task_id='task1') for _ in range(2)]
            for job in jobs:
                await store.add_job(job)

            # The first worker gets the first job in the queue
            jobs1 = await store.acquire_jobs('worker1', 1)
            assert len(jobs1) == 1
            assert jobs1[0].id == jobs[0].id

            # The second worker gets the second job
            jobs2 = await store.acquire_jobs('worker2', 1)
            assert len(jobs2) == 1
            assert jobs2[0].id == jobs[1].id

            # The third worker gets nothing
            jobs3 = await store.acquire_jobs('worker3', 1)
            assert not jobs3

    async def test_job_release_success(
            self, datastore_cm: AsyncContextManager[AsyncDataStore]) -> None:
        async with datastore_cm as store:
            await store.add_task(Task(id='task1', func=asynccontextmanager))
            job = Job(task_id='task1')
            await store.add_job(job)

            acquired = await store.acquire_jobs('worker_id', 2)
            assert len(acquired) == 1
            assert acquired[0].id == job.id

            await store.release_job('worker_id', acquired[0],
                                    JobResult(JobOutcome.success, return_value='foo'))
            result = await store.get_job_result(acquired[0].id)
            assert result.outcome is JobOutcome.success
            assert result.exception is None
            assert result.return_value == 'foo'

            # Check that the job and its result are gone
            assert not await store.get_jobs({acquired[0].id})
            assert not await store.get_job_result(acquired[0].id)

    async def test_job_release_failure(
            self, datastore_cm: AsyncContextManager[AsyncDataStore]) -> None:
        async with datastore_cm as store:
            await store.add_task(Task(id='task1', func=asynccontextmanager))
            job = Job(task_id='task1')
            await store.add_job(job)

            acquired = await store.acquire_jobs('worker_id', 2)
            assert len(acquired) == 1
            assert acquired[0].id == job.id

            await store.release_job('worker_id', acquired[0],
                                    JobResult(JobOutcome.failure, exception=ValueError('foo')))
            result = await store.get_job_result(acquired[0].id)
            assert result.outcome is JobOutcome.failure
            assert isinstance(result.exception, ValueError)
            assert result.exception.args == ('foo',)
            assert result.return_value is None

            # Check that the job and its result are gone
            assert not await store.get_jobs({acquired[0].id})
            assert not await store.get_job_result(acquired[0].id)

    async def test_job_release_missed_deadline(
            self, datastore_cm: AsyncContextManager[AsyncDataStore]):
        async with datastore_cm as store:
            await store.add_task(Task(id='task1', func=asynccontextmanager))
            job = Job(task_id='task1')
            await store.add_job(job)

            acquired = await store.acquire_jobs('worker_id', 2)
            assert len(acquired) == 1
            assert acquired[0].id == job.id

            await store.release_job('worker_id', acquired[0],
                                    JobResult(JobOutcome.missed_start_deadline))
            result = await store.get_job_result(acquired[0].id)
            assert result.outcome is JobOutcome.missed_start_deadline
            assert result.exception is None
            assert result.return_value is None

            # Check that the job and its result are gone
            assert not await store.get_jobs({acquired[0].id})
            assert not await store.get_job_result(acquired[0].id)

    async def test_job_release_cancelled(
            self, datastore_cm: AsyncContextManager[AsyncDataStore]) -> None:
        async with datastore_cm as store:
            await store.add_task(Task(id='task1', func=asynccontextmanager))
            job = Job(task_id='task1')
            await store.add_job(job)

            acquired = await store.acquire_jobs('worker1', 2)
            assert len(acquired) == 1
            assert acquired[0].id == job.id

            await store.release_job('worker1', acquired[0], JobResult(JobOutcome.cancelled))
            result = await store.get_job_result(acquired[0].id)
            assert result.outcome is JobOutcome.cancelled
            assert result.exception is None
            assert result.return_value is None

            # Check that the job and its result are gone
            assert not await store.get_jobs({acquired[0].id})
            assert not await store.get_job_result(acquired[0].id)

    async def test_acquire_jobs_lock_timeout(
            self, datastore_cm: AsyncContextManager[AsyncDataStore],
            freezer: FrozenDateTimeFactory) -> None:
        """
        Test that a worker can acquire jobs that were acquired by another scheduler but not
        released within the lock timeout period.

        """
        async with datastore_cm as store:
            await store.add_task(Task(id='task1', func=asynccontextmanager))
            job = Job(task_id='task1')
            await store.add_job(job)

            # First, one worker acquires the first available job
            acquired = await store.acquire_jobs('worker1', 1)
            assert len(acquired) == 1
            assert acquired[0].id == job.id

            # Try to acquire the job just at the threshold (now == acquired_until).
            # This should not yield any jobs.
            freezer.tick(30)
            assert not await store.acquire_jobs('worker2', 1)

            # Right after that, the job should be available
            freezer.tick(1)
            acquired = await store.acquire_jobs('worker2', 1)
            assert len(acquired) == 1
            assert acquired[0].id == job.id

    async def test_acquire_jobs_max_number_exceeded(
            self, datastore_cm: AsyncContextManager[AsyncDataStore]) -> None:
        async with datastore_cm as store:
            await store.add_task(Task(id='task1', func=asynccontextmanager, max_running_jobs=2))
            jobs = [Job(task_id='task1'), Job(task_id='task1'), Job(task_id='task1')]
            for job in jobs:
                await store.add_job(job)

            # Check that only 2 jobs are returned from acquire_jobs() even though the limit wqas 3
            acquired_jobs = await store.acquire_jobs('worker1', 3)
            assert [job.id for job in acquired_jobs] == [job.id for job in jobs[:2]]

            # Release one job, and the worker should be able to acquire the third job
            await store.release_job('worker1', acquired_jobs[0],
                                    JobResult(outcome=JobOutcome.success, return_value=None))
            acquired_jobs = await store.acquire_jobs('worker1', 3)
            assert [job.id for job in acquired_jobs] == [jobs[2].id]
