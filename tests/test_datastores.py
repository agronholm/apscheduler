from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import AsyncContextManager, AsyncGenerator, List, Optional, Set, Type

import anyio
import pytest
from freezegun.api import FrozenDateTimeFactory

from apscheduler.abc import AsyncDataStore, Job, Schedule
from apscheduler.events import Event, JobAdded, ScheduleAdded, ScheduleRemoved, ScheduleUpdated
from apscheduler.policies import CoalescePolicy, ConflictPolicy
from apscheduler.triggers.date import DateTrigger


@pytest.fixture
def schedules() -> List[Schedule]:
    trigger = DateTrigger(datetime(2020, 9, 13, tzinfo=timezone.utc))
    schedule1 = Schedule(id='s1', task_id='bogus', trigger=trigger, args=(), kwargs={},
                         coalesce=CoalescePolicy.latest, misfire_grace_time=None, tags=frozenset())
    schedule1.next_fire_time = trigger.next()

    trigger = DateTrigger(datetime(2020, 9, 14, tzinfo=timezone.utc))
    schedule2 = Schedule(id='s2', task_id='bogus', trigger=trigger, args=(), kwargs={},
                         coalesce=CoalescePolicy.latest, misfire_grace_time=None, tags=frozenset())
    schedule2.next_fire_time = trigger.next()

    trigger = DateTrigger(datetime(2020, 9, 15, tzinfo=timezone.utc))
    schedule3 = Schedule(id='s3', task_id='bogus', trigger=trigger, args=(), kwargs={},
                         coalesce=CoalescePolicy.latest, misfire_grace_time=None, tags=frozenset())
    return [schedule1, schedule2, schedule3]


@pytest.fixture
def jobs() -> List[Job]:
    job1 = Job('task1', print, ('hello',), {'arg2': 'world'}, 'schedule1',
               datetime(2020, 10, 10, tzinfo=timezone.utc),
               datetime(2020, 10, 10, 1, tzinfo=timezone.utc), frozenset())
    job2 = Job('task2', print, ('hello',), {'arg2': 'world'}, None,
               None, None, frozenset())
    return [job1, job2]


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
            assert not await store.acquire_schedules('dummy-id3', 1)

            # The schedules here have run their course, and releasing them should delete them
            schedules1[0].next_fire_time = None
            schedules2[0].next_fire_time = datetime(2020, 9, 15, tzinfo=timezone.utc)
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
        Test that a scheduler can acquire schedules that were acquired by another scheduler but not
        released within the lock timeout period.

        """
        async with datastore_cm as store:
            # First, one scheduler acquires the first available schedule
            await store.add_schedule(schedules[0], ConflictPolicy.exception)
            acquired = await store.acquire_schedules('dummy-id1', 1)
            assert len(acquired) == 1
            assert acquired[0].id == 's1'

            # Try to acquire the schedule just at the threshold (now == acquired_until).
            # This should not yield any schedules.
            freezer.tick(30)
            assert not await store.acquire_schedules('dummy-id2', 1)

            # Right after that, the schedule should be available
            freezer.tick(1)
            acquired = await store.acquire_schedules('dummy-id2', 1)
            assert len(acquired) == 1
            assert acquired[0].id == 's1'

    async def test_acquire_release_jobs(self, datastore_cm: AsyncContextManager[AsyncDataStore],
                                        jobs: List[Job]) -> None:
        async with datastore_cm as store, capture_events(store, 0) as events:
            for job in jobs:
                await store.add_job(job)

            # The first worker gets the first job in the queue
            jobs1 = await store.acquire_jobs('dummy-id1', 1)
            assert len(jobs1) == 1
            assert jobs1[0].id == jobs[0].id

            # The second worker gets the second job
            jobs2 = await store.acquire_jobs('dummy-id2', 1)
            assert len(jobs2) == 1
            assert jobs2[0].id == jobs[1].id

            # The third worker gets nothing
            assert not await store.acquire_jobs('dummy-id3', 1)

            # All the jobs should still be returned
            visible_jobs = await store.get_jobs()
            assert len(visible_jobs) == 2

            await store.release_jobs('dummy-id1', jobs1)
            await store.release_jobs('dummy-id2', jobs2)

            # All the jobs should be gone
            visible_jobs = await store.get_jobs()
            assert len(visible_jobs) == 0

        # Check for the appropriate events
        assert all(isinstance(event, JobAdded) for event in events)

    async def test_acquire_jobs_lock_timeout(
            self, datastore_cm: AsyncContextManager[AsyncDataStore], jobs: List[Job],
            freezer: FrozenDateTimeFactory) -> None:
        """
        Test that a worker can acquire jobs that were acquired by another scheduler but not
        released within the lock timeout period.

        """
        async with datastore_cm as store:
            # First, one worker acquires the first available job
            await store.add_job(jobs[0])
            acquired = await store.acquire_jobs('dummy-id1', 1)
            assert len(acquired) == 1
            assert acquired[0].id == jobs[0].id

            # Try to acquire the job just at the threshold (now == acquired_until).
            # This should not yield any jobs.
            freezer.tick(30)
            assert not await store.acquire_jobs('dummy-id2', 1)

            # Right after that, the job should be available
            freezer.tick(1)
            acquired = await store.acquire_jobs('dummy-id2', 1)
            assert len(acquired) == 1
            assert acquired[0].id == jobs[0].id
