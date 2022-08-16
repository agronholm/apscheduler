from __future__ import annotations

import threading
from collections.abc import Generator
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timedelta, timezone
from tempfile import TemporaryDirectory
from typing import Any, AsyncGenerator, cast

import anyio
import pytest
from _pytest.fixtures import SubRequest
from anyio import CancelScope
from freezegun.api import FrozenDateTimeFactory
from pytest_lazyfixture import lazy_fixture

from apscheduler import (
    CoalescePolicy,
    ConflictPolicy,
    Event,
    JobOutcome,
    ScheduleAdded,
    ScheduleRemoved,
    ScheduleUpdated,
    TaskAdded,
    TaskLookupError,
    TaskUpdated,
)
from apscheduler._structures import Job, JobResult, Schedule, Task
from apscheduler.abc import AsyncDataStore, AsyncEventBroker, DataStore, EventBroker
from apscheduler.datastores.async_adapter import AsyncDataStoreAdapter
from apscheduler.datastores.memory import MemoryDataStore
from apscheduler.eventbrokers.async_local import LocalAsyncEventBroker
from apscheduler.eventbrokers.local import LocalEventBroker
from apscheduler.triggers.date import DateTrigger


@pytest.fixture
def memory_store() -> DataStore:
    yield MemoryDataStore()


@pytest.fixture
def adapted_memory_store() -> AsyncDataStore:
    store = MemoryDataStore()
    return AsyncDataStoreAdapter(store)


@pytest.fixture
def mongodb_store() -> DataStore:
    from pymongo import MongoClient

    from apscheduler.datastores.mongodb import MongoDBDataStore

    with MongoClient(tz_aware=True, serverSelectionTimeoutMS=1000) as client:
        yield MongoDBDataStore(client, start_from_scratch=True)


@pytest.fixture
def sqlite_store() -> DataStore:
    from sqlalchemy.future import create_engine

    from apscheduler.datastores.sqlalchemy import SQLAlchemyDataStore

    with TemporaryDirectory("sqlite_") as tempdir:
        engine = create_engine(f"sqlite:///{tempdir}/test.db")
        try:
            yield SQLAlchemyDataStore(engine)
        finally:
            engine.dispose()


@pytest.fixture
def psycopg2_store() -> DataStore:
    from sqlalchemy.future import create_engine

    from apscheduler.datastores.sqlalchemy import SQLAlchemyDataStore

    engine = create_engine("postgresql+psycopg2://postgres:secret@localhost/testdb")
    try:
        yield SQLAlchemyDataStore(engine, start_from_scratch=True)
    finally:
        engine.dispose()


@pytest.fixture
def mysql_store() -> DataStore:
    from sqlalchemy.future import create_engine

    from apscheduler.datastores.sqlalchemy import SQLAlchemyDataStore

    engine = create_engine("mysql+pymysql://root:secret@localhost/testdb")
    try:
        yield SQLAlchemyDataStore(engine, start_from_scratch=True)
    finally:
        engine.dispose()


@pytest.fixture
async def asyncpg_store() -> AsyncDataStore:
    from sqlalchemy.ext.asyncio import create_async_engine

    from apscheduler.datastores.async_sqlalchemy import AsyncSQLAlchemyDataStore

    engine = create_async_engine(
        "postgresql+asyncpg://postgres:secret@localhost/testdb", future=True
    )
    try:
        yield AsyncSQLAlchemyDataStore(engine, start_from_scratch=True)
    finally:
        await engine.dispose()


@pytest.fixture
async def asyncmy_store() -> AsyncDataStore:
    from sqlalchemy.ext.asyncio import create_async_engine

    from apscheduler.datastores.async_sqlalchemy import AsyncSQLAlchemyDataStore

    engine = create_async_engine(
        "mysql+asyncmy://root:secret@localhost/testdb?charset=utf8mb4", future=True
    )
    try:
        yield AsyncSQLAlchemyDataStore(engine, start_from_scratch=True)
    finally:
        await engine.dispose()


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
    return [schedule1, schedule2, schedule3]


class TestDataStores:
    @contextmanager
    def capture_events(
        self,
        datastore: DataStore,
        limit: int,
        event_types: set[type[Event]] | None = None,
    ) -> Generator[list[Event], None, None]:
        def listener(event: Event) -> None:
            events.append(event)
            if len(events) == limit:
                limit_event.set()
                subscription.unsubscribe()

        events: list[Event] = []
        limit_event = threading.Event()
        subscription = datastore.events.subscribe(listener, event_types)
        yield events
        if limit:
            limit_event.wait(2)

    @pytest.fixture
    def event_broker(self) -> Generator[EventBroker, Any, None]:
        broker = LocalEventBroker()
        broker.start()
        yield broker
        broker.stop()

    @pytest.fixture(
        params=[
            pytest.param(lazy_fixture("memory_store"), id="memory"),
            pytest.param(lazy_fixture("sqlite_store"), id="sqlite"),
            pytest.param(
                lazy_fixture("mongodb_store"),
                id="mongodb",
                marks=[pytest.mark.external_service],
            ),
            pytest.param(
                lazy_fixture("psycopg2_store"),
                id="psycopg2",
                marks=[pytest.mark.external_service],
            ),
            pytest.param(
                lazy_fixture("mysql_store"),
                id="mysql",
                marks=[pytest.mark.external_service],
            ),
        ]
    )
    def datastore(
        self, request: SubRequest, event_broker: EventBroker
    ) -> Generator[DataStore, Any, None]:
        datastore = cast(DataStore, request.param)
        datastore.start(event_broker)
        yield datastore
        datastore.stop()

    def test_add_replace_task(self, datastore: DataStore) -> None:
        import math

        event_types = {TaskAdded, TaskUpdated}
        with self.capture_events(datastore, 3, event_types) as events:
            datastore.add_task(Task(id="test_task", func=print))
            datastore.add_task(Task(id="test_task2", func=math.ceil))
            datastore.add_task(Task(id="test_task", func=repr))

            tasks = datastore.get_tasks()
            assert len(tasks) == 2
            assert tasks[0].id == "test_task"
            assert tasks[0].func is repr
            assert tasks[1].id == "test_task2"
            assert tasks[1].func is math.ceil

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

    def test_add_schedules(
        self, datastore: DataStore, schedules: list[Schedule]
    ) -> None:
        with self.capture_events(datastore, 3, {ScheduleAdded}) as events:
            for schedule in schedules:
                datastore.add_schedule(schedule, ConflictPolicy.exception)

            assert datastore.get_schedules() == schedules
            assert datastore.get_schedules({"s1", "s2", "s3"}) == schedules
            assert datastore.get_schedules({"s1"}) == [schedules[0]]
            assert datastore.get_schedules({"s2"}) == [schedules[1]]
            assert datastore.get_schedules({"s3"}) == [schedules[2]]

        for event, schedule in zip(events, schedules):
            assert event.schedule_id == schedule.id
            assert event.next_fire_time == schedule.next_fire_time

    def test_replace_schedules(
        self, datastore: DataStore, schedules: list[Schedule]
    ) -> None:
        with self.capture_events(datastore, 1, {ScheduleUpdated}) as events:
            for schedule in schedules:
                datastore.add_schedule(schedule, ConflictPolicy.exception)

            next_fire_time = schedules[2].trigger.next()
            schedule = Schedule(
                id="s3",
                task_id="foo",
                trigger=schedules[2].trigger,
                args=(),
                kwargs={},
                coalesce=CoalescePolicy.earliest,
                misfire_grace_time=None,
                tags=frozenset(),
            )
            schedule.next_fire_time = next_fire_time
            datastore.add_schedule(schedule, ConflictPolicy.replace)

            schedules = datastore.get_schedules({schedule.id})
            assert schedules[0].task_id == "foo"
            assert schedules[0].next_fire_time == next_fire_time
            assert schedules[0].args == ()
            assert schedules[0].kwargs == {}
            assert schedules[0].coalesce is CoalescePolicy.earliest
            assert schedules[0].misfire_grace_time is None
            assert schedules[0].tags == frozenset()

        received_event = events.pop(0)
        assert received_event.schedule_id == "s3"
        assert received_event.next_fire_time == datetime(
            2020, 9, 15, tzinfo=timezone.utc
        )
        assert not events

    def test_remove_schedules(
        self, datastore: DataStore, schedules: list[Schedule]
    ) -> None:
        with self.capture_events(datastore, 2, {ScheduleRemoved}) as events:
            for schedule in schedules:
                datastore.add_schedule(schedule, ConflictPolicy.exception)

            datastore.remove_schedules(["s1", "s2"])
            assert datastore.get_schedules() == [schedules[2]]

        received_event = events.pop(0)
        assert received_event.schedule_id == "s1"

        received_event = events.pop(0)
        assert received_event.schedule_id == "s2"

        assert not events

    @pytest.mark.freeze_time(datetime(2020, 9, 14, tzinfo=timezone.utc))
    def test_acquire_release_schedules(
        self, datastore: DataStore, schedules: list[Schedule]
    ) -> None:
        event_types = {ScheduleRemoved, ScheduleUpdated}
        with self.capture_events(datastore, 2, event_types) as events:
            for schedule in schedules:
                datastore.add_schedule(schedule, ConflictPolicy.exception)

            # The first scheduler gets the first due schedule
            schedules1 = datastore.acquire_schedules("dummy-id1", 1)
            assert len(schedules1) == 1
            assert schedules1[0].id == "s1"

            # The second scheduler gets the second due schedule
            schedules2 = datastore.acquire_schedules("dummy-id2", 1)
            assert len(schedules2) == 1
            assert schedules2[0].id == "s2"

            # The third scheduler gets nothing
            schedules3 = datastore.acquire_schedules("dummy-id3", 1)
            assert not schedules3

            # Update the schedules and check that the job store actually deletes the
            # first one and updates the second one
            schedules1[0].next_fire_time = None
            schedules2[0].next_fire_time = datetime(2020, 9, 15, tzinfo=timezone.utc)

            # Release all the schedules
            datastore.release_schedules("dummy-id1", schedules1)
            datastore.release_schedules("dummy-id2", schedules2)

            # Check that the first schedule is gone
            schedules = datastore.get_schedules()
            assert len(schedules) == 2
            assert schedules[0].id == "s2"
            assert schedules[1].id == "s3"

        # Check for the appropriate update and delete events
        received_event = events.pop(0)
        assert isinstance(received_event, ScheduleRemoved)
        assert received_event.schedule_id == "s1"

        received_event = events.pop(0)
        assert isinstance(received_event, ScheduleUpdated)
        assert received_event.schedule_id == "s2"
        assert received_event.next_fire_time == datetime(
            2020, 9, 15, tzinfo=timezone.utc
        )

        assert not events

    def test_release_schedule_two_identical_fire_times(
        self, datastore: DataStore
    ) -> None:
        """Regression test for #616."""
        for i in range(1, 3):
            trigger = DateTrigger(datetime(2020, 9, 13, tzinfo=timezone.utc))
            schedule = Schedule(id=f"s{i}", task_id="task1", trigger=trigger)
            schedule.next_fire_time = trigger.next()
            datastore.add_schedule(schedule, ConflictPolicy.exception)

        schedules = datastore.acquire_schedules("foo", 3)
        schedules[0].next_fire_time = None
        datastore.release_schedules("foo", schedules)

        remaining = datastore.get_schedules({s.id for s in schedules})
        assert len(remaining) == 1
        assert remaining[0].id == schedules[1].id

    def test_release_two_schedules_at_once(self, datastore: DataStore) -> None:
        """Regression test for #621."""
        for i in range(2):
            trigger = DateTrigger(datetime(2020, 9, 13, tzinfo=timezone.utc))
            schedule = Schedule(id=f"s{i}", task_id="task1", trigger=trigger)
            schedule.next_fire_time = trigger.next()
            datastore.add_schedule(schedule, ConflictPolicy.exception)

        schedules = datastore.acquire_schedules("foo", 3)
        datastore.release_schedules("foo", schedules)

        remaining = datastore.get_schedules({s.id for s in schedules})
        assert len(remaining) == 2

    def test_acquire_schedules_lock_timeout(
        self, datastore: DataStore, schedules: list[Schedule], freezer
    ) -> None:
        """
        Test that a scheduler can acquire schedules that were acquired by another
        scheduler but not released within the lock timeout period.

        """
        datastore.add_schedule(schedules[0], ConflictPolicy.exception)

        # First, one scheduler acquires the first available schedule
        acquired1 = datastore.acquire_schedules("dummy-id1", 1)
        assert len(acquired1) == 1
        assert acquired1[0].id == "s1"

        # Try to acquire the schedule just at the threshold (now == acquired_until).
        # This should not yield any schedules.
        freezer.tick(30)
        acquired2 = datastore.acquire_schedules("dummy-id2", 1)
        assert not acquired2

        # Right after that, the schedule should be available
        freezer.tick(1)
        acquired3 = datastore.acquire_schedules("dummy-id2", 1)
        assert len(acquired3) == 1
        assert acquired3[0].id == "s1"

    def test_acquire_multiple_workers(self, datastore: DataStore) -> None:
        datastore.add_task(Task(id="task1", func=asynccontextmanager))
        jobs = [Job(task_id="task1") for _ in range(2)]
        for job in jobs:
            datastore.add_job(job)

        # The first worker gets the first job in the queue
        jobs1 = datastore.acquire_jobs("worker1", 1)
        assert len(jobs1) == 1
        assert jobs1[0].id == jobs[0].id

        # The second worker gets the second job
        jobs2 = datastore.acquire_jobs("worker2", 1)
        assert len(jobs2) == 1
        assert jobs2[0].id == jobs[1].id

        # The third worker gets nothing
        jobs3 = datastore.acquire_jobs("worker3", 1)
        assert not jobs3

    def test_job_release_success(self, datastore: DataStore) -> None:
        datastore.add_task(Task(id="task1", func=asynccontextmanager))
        job = Job(task_id="task1", result_expiration_time=timedelta(minutes=1))
        datastore.add_job(job)

        acquired = datastore.acquire_jobs("worker_id", 2)
        assert len(acquired) == 1
        assert acquired[0].id == job.id

        datastore.release_job(
            "worker_id",
            acquired[0].task_id,
            JobResult.from_job(
                acquired[0],
                JobOutcome.success,
                return_value="foo",
            ),
        )
        result = datastore.get_job_result(acquired[0].id)
        assert result.outcome is JobOutcome.success
        assert result.exception is None
        assert result.return_value == "foo"

        # Check that the job and its result are gone
        assert not datastore.get_jobs({acquired[0].id})
        assert not datastore.get_job_result(acquired[0].id)

    def test_job_release_failure(self, datastore: DataStore) -> None:
        datastore.add_task(Task(id="task1", func=asynccontextmanager))
        job = Job(task_id="task1", result_expiration_time=timedelta(minutes=1))
        datastore.add_job(job)

        acquired = datastore.acquire_jobs("worker_id", 2)
        assert len(acquired) == 1
        assert acquired[0].id == job.id

        datastore.release_job(
            "worker_id",
            acquired[0].task_id,
            JobResult.from_job(
                acquired[0],
                JobOutcome.error,
                exception=ValueError("foo"),
            ),
        )
        result = datastore.get_job_result(acquired[0].id)
        assert result.outcome is JobOutcome.error
        assert isinstance(result.exception, ValueError)
        assert result.exception.args == ("foo",)
        assert result.return_value is None

        # Check that the job and its result are gone
        assert not datastore.get_jobs({acquired[0].id})
        assert not datastore.get_job_result(acquired[0].id)

    def test_job_release_missed_deadline(self, datastore: DataStore):
        datastore.add_task(Task(id="task1", func=asynccontextmanager))
        job = Job(task_id="task1", result_expiration_time=timedelta(minutes=1))
        datastore.add_job(job)

        acquired = datastore.acquire_jobs("worker_id", 2)
        assert len(acquired) == 1
        assert acquired[0].id == job.id

        datastore.release_job(
            "worker_id",
            acquired[0].task_id,
            JobResult.from_job(
                acquired[0],
                JobOutcome.missed_start_deadline,
            ),
        )
        result = datastore.get_job_result(acquired[0].id)
        assert result.outcome is JobOutcome.missed_start_deadline
        assert result.exception is None
        assert result.return_value is None

        # Check that the job and its result are gone
        assert not datastore.get_jobs({acquired[0].id})
        assert not datastore.get_job_result(acquired[0].id)

    def test_job_release_cancelled(self, datastore: DataStore) -> None:
        datastore.add_task(Task(id="task1", func=asynccontextmanager))
        job = Job(task_id="task1", result_expiration_time=timedelta(minutes=1))
        datastore.add_job(job)

        acquired = datastore.acquire_jobs("worker1", 2)
        assert len(acquired) == 1
        assert acquired[0].id == job.id

        datastore.release_job(
            "worker1",
            acquired[0].task_id,
            JobResult.from_job(
                acquired[0],
                JobOutcome.cancelled,
            ),
        )
        result = datastore.get_job_result(acquired[0].id)
        assert result.outcome is JobOutcome.cancelled
        assert result.exception is None
        assert result.return_value is None

        # Check that the job and its result are gone
        assert not datastore.get_jobs({acquired[0].id})
        assert not datastore.get_job_result(acquired[0].id)

    def test_acquire_jobs_lock_timeout(
        self, datastore: DataStore, freezer: FrozenDateTimeFactory
    ) -> None:
        """
        Test that a worker can acquire jobs that were acquired by another scheduler but
        not released within the lock timeout period.

        """
        datastore.add_task(Task(id="task1", func=asynccontextmanager))
        job = Job(task_id="task1")
        datastore.add_job(job)

        # First, one worker acquires the first available job
        acquired = datastore.acquire_jobs("worker1", 1)
        assert len(acquired) == 1
        assert acquired[0].id == job.id

        # Try to acquire the job just at the threshold (now == acquired_until).
        # This should not yield any jobs.
        freezer.tick(30)
        assert not datastore.acquire_jobs("worker2", 1)

        # Right after that, the job should be available
        freezer.tick(1)
        acquired = datastore.acquire_jobs("worker2", 1)
        assert len(acquired) == 1
        assert acquired[0].id == job.id

    def test_acquire_jobs_max_number_exceeded(self, datastore: DataStore) -> None:
        datastore.add_task(
            Task(id="task1", func=asynccontextmanager, max_running_jobs=2)
        )
        jobs = [Job(task_id="task1"), Job(task_id="task1"), Job(task_id="task1")]
        for job in jobs:
            datastore.add_job(job)

        # Check that only 2 jobs are returned from acquire_jobs() even though the limit
        # wqas 3
        acquired_jobs = datastore.acquire_jobs("worker1", 3)
        assert [job.id for job in acquired_jobs] == [job.id for job in jobs[:2]]

        # Release one job, and the worker should be able to acquire the third job
        datastore.release_job(
            "worker1",
            acquired_jobs[0].task_id,
            JobResult.from_job(
                acquired_jobs[0],
                JobOutcome.success,
                return_value=None,
            ),
        )
        acquired_jobs = datastore.acquire_jobs("worker1", 3)
        assert [job.id for job in acquired_jobs] == [jobs[2].id]

    def test_add_get_task(self, datastore: DataStore) -> None:
        with pytest.raises(TaskLookupError):
            datastore.get_task("dummyid")

        datastore.add_task(Task(id="dummyid", func=asynccontextmanager))
        task = datastore.get_task("dummyid")
        assert task.id == "dummyid"
        assert task.func is asynccontextmanager


@pytest.mark.anyio
class TestAsyncDataStores:
    @asynccontextmanager
    async def capture_events(
        self,
        datastore: AsyncDataStore,
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
        subscription = datastore.events.subscribe(listener, event_types)
        yield events
        if limit:
            with anyio.fail_after(3):
                await limit_event.wait()

    @pytest.fixture
    async def event_broker(self) -> AsyncGenerator[AsyncEventBroker, Any]:
        broker = LocalAsyncEventBroker()
        await broker.start()
        yield broker
        await broker.stop()

    @pytest.fixture(
        params=[
            pytest.param(lazy_fixture("adapted_memory_store"), id="memory"),
            pytest.param(
                lazy_fixture("asyncpg_store"),
                id="asyncpg",
                marks=[pytest.mark.external_service],
            ),
            pytest.param(
                lazy_fixture("asyncmy_store"),
                id="asyncmy",
                marks=[pytest.mark.external_service],
            ),
        ]
    )
    async def raw_datastore(
        self, request: SubRequest, event_broker: AsyncEventBroker
    ) -> AsyncDataStore:
        return cast(AsyncDataStore, request.param)

    @pytest.fixture
    async def datastore(
        self, raw_datastore: AsyncDataStore, event_broker: AsyncEventBroker
    ) -> AsyncGenerator[AsyncDataStore, Any]:
        await raw_datastore.start(event_broker)
        yield raw_datastore
        await raw_datastore.stop()

    async def test_add_replace_task(self, datastore: AsyncDataStore) -> None:
        import math

        event_types = {TaskAdded, TaskUpdated}
        async with self.capture_events(datastore, 3, event_types) as events:
            await datastore.add_task(Task(id="test_task", func=print))
            await datastore.add_task(Task(id="test_task2", func=math.ceil))
            await datastore.add_task(Task(id="test_task", func=repr))

            tasks = await datastore.get_tasks()
            assert len(tasks) == 2
            assert tasks[0].id == "test_task"
            assert tasks[0].func is repr
            assert tasks[1].id == "test_task2"
            assert tasks[1].func is math.ceil

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

    async def test_add_schedules(
        self, datastore: AsyncDataStore, schedules: list[Schedule]
    ) -> None:
        async with self.capture_events(datastore, 3, {ScheduleAdded}) as events:
            for schedule in schedules:
                await datastore.add_schedule(schedule, ConflictPolicy.exception)

            assert await datastore.get_schedules() == schedules
            assert await datastore.get_schedules({"s1", "s2", "s3"}) == schedules
            assert await datastore.get_schedules({"s1"}) == [schedules[0]]
            assert await datastore.get_schedules({"s2"}) == [schedules[1]]
            assert await datastore.get_schedules({"s3"}) == [schedules[2]]

        for event, schedule in zip(events, schedules):
            assert event.schedule_id == schedule.id
            assert event.next_fire_time == schedule.next_fire_time

    async def test_replace_schedules(
        self, datastore: AsyncDataStore, schedules: list[Schedule]
    ) -> None:
        async with self.capture_events(datastore, 1, {ScheduleUpdated}) as events:
            for schedule in schedules:
                await datastore.add_schedule(schedule, ConflictPolicy.exception)

            next_fire_time = schedules[2].trigger.next()
            schedule = Schedule(
                id="s3",
                task_id="foo",
                trigger=schedules[2].trigger,
                args=(),
                kwargs={},
                coalesce=CoalescePolicy.earliest,
                misfire_grace_time=None,
                tags=frozenset(),
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
            assert schedules[0].tags == frozenset()

        received_event = events.pop(0)
        assert received_event.schedule_id == "s3"
        assert received_event.next_fire_time == datetime(
            2020, 9, 15, tzinfo=timezone.utc
        )
        assert not events

    async def test_remove_schedules(
        self, datastore: AsyncDataStore, schedules: list[Schedule]
    ) -> None:
        async with self.capture_events(datastore, 2, {ScheduleRemoved}) as events:
            for schedule in schedules:
                await datastore.add_schedule(schedule, ConflictPolicy.exception)

            await datastore.remove_schedules(["s1", "s2"])
            assert await datastore.get_schedules() == [schedules[2]]

        received_event = events.pop(0)
        assert received_event.schedule_id == "s1"

        received_event = events.pop(0)
        assert received_event.schedule_id == "s2"

        assert not events

    @pytest.mark.freeze_time(datetime(2020, 9, 14, tzinfo=timezone.utc))
    async def test_acquire_release_schedules(
        self, datastore: AsyncDataStore, schedules: list[Schedule]
    ) -> None:
        event_types = {ScheduleRemoved, ScheduleUpdated}
        async with self.capture_events(datastore, 2, event_types) as events:
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

            # Check that the first schedule is gone
            schedules = await datastore.get_schedules()
            assert len(schedules) == 2
            assert schedules[0].id == "s2"
            assert schedules[1].id == "s3"

        # Check for the appropriate update and delete events
        received_event = events.pop(0)
        assert isinstance(received_event, ScheduleRemoved)
        assert received_event.schedule_id == "s1"

        received_event = events.pop(0)
        assert isinstance(received_event, ScheduleUpdated)
        assert received_event.schedule_id == "s2"
        assert received_event.next_fire_time == datetime(
            2020, 9, 15, tzinfo=timezone.utc
        )

        assert not events

    async def test_release_schedule_two_identical_fire_times(
        self, datastore: AsyncDataStore
    ) -> None:
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
        assert len(remaining) == 1
        assert remaining[0].id == schedules[1].id

    async def test_release_two_schedules_at_once(
        self, datastore: AsyncDataStore
    ) -> None:
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

    async def test_acquire_schedules_lock_timeout(
        self, datastore: AsyncDataStore, schedules: list[Schedule], freezer
    ) -> None:
        """
        Test that a scheduler can acquire schedules that were acquired by another
        scheduler but not released within the lock timeout period.

        """
        await datastore.add_schedule(schedules[0], ConflictPolicy.exception)

        # First, one scheduler acquires the first available schedule
        acquired1 = await datastore.acquire_schedules("dummy-id1", 1)
        assert len(acquired1) == 1
        assert acquired1[0].id == "s1"

        # Try to acquire the schedule just at the threshold (now == acquired_until).
        # This should not yield any schedules.
        freezer.tick(30)
        acquired2 = await datastore.acquire_schedules("dummy-id2", 1)
        assert not acquired2

        # Right after that, the schedule should be available
        freezer.tick(1)
        acquired3 = await datastore.acquire_schedules("dummy-id2", 1)
        assert len(acquired3) == 1
        assert acquired3[0].id == "s1"

    async def test_acquire_multiple_workers(self, datastore: AsyncDataStore) -> None:
        await datastore.add_task(Task(id="task1", func=asynccontextmanager))
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

    async def test_job_release_success(self, datastore: AsyncDataStore) -> None:
        await datastore.add_task(Task(id="task1", func=asynccontextmanager))
        job = Job(task_id="task1", result_expiration_time=timedelta(minutes=1))
        await datastore.add_job(job)

        acquired = await datastore.acquire_jobs("worker_id", 2)
        assert len(acquired) == 1
        assert acquired[0].id == job.id

        await datastore.release_job(
            "worker_id",
            acquired[0].task_id,
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

    async def test_job_release_failure(self, datastore: AsyncDataStore) -> None:
        await datastore.add_task(Task(id="task1", func=asynccontextmanager))
        job = Job(task_id="task1", result_expiration_time=timedelta(minutes=1))
        await datastore.add_job(job)

        acquired = await datastore.acquire_jobs("worker_id", 2)
        assert len(acquired) == 1
        assert acquired[0].id == job.id

        await datastore.release_job(
            "worker_id",
            acquired[0].task_id,
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

    async def test_job_release_missed_deadline(self, datastore: AsyncDataStore):
        await datastore.add_task(Task(id="task1", func=asynccontextmanager))
        job = Job(task_id="task1", result_expiration_time=timedelta(minutes=1))
        await datastore.add_job(job)

        acquired = await datastore.acquire_jobs("worker_id", 2)
        assert len(acquired) == 1
        assert acquired[0].id == job.id

        await datastore.release_job(
            "worker_id",
            acquired[0].task_id,
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

    async def test_job_release_cancelled(self, datastore: AsyncDataStore) -> None:
        await datastore.add_task(Task(id="task1", func=asynccontextmanager))
        job = Job(task_id="task1", result_expiration_time=timedelta(minutes=1))
        await datastore.add_job(job)

        acquired = await datastore.acquire_jobs("worker1", 2)
        assert len(acquired) == 1
        assert acquired[0].id == job.id

        await datastore.release_job(
            "worker1",
            acquired[0].task_id,
            JobResult.from_job(acquired[0], JobOutcome.cancelled),
        )
        result = await datastore.get_job_result(acquired[0].id)
        assert result.outcome is JobOutcome.cancelled
        assert result.exception is None
        assert result.return_value is None

        # Check that the job and its result are gone
        assert not await datastore.get_jobs({acquired[0].id})
        assert not await datastore.get_job_result(acquired[0].id)

    async def test_acquire_jobs_lock_timeout(
        self, datastore: AsyncDataStore, freezer: FrozenDateTimeFactory
    ) -> None:
        """
        Test that a worker can acquire jobs that were acquired by another scheduler but
        not released within the lock timeout period.

        """
        await datastore.add_task(Task(id="task1", func=asynccontextmanager))
        job = Job(task_id="task1", result_expiration_time=timedelta(minutes=1))
        await datastore.add_job(job)

        # First, one worker acquires the first available job
        acquired = await datastore.acquire_jobs("worker1", 1)
        assert len(acquired) == 1
        assert acquired[0].id == job.id

        # Try to acquire the job just at the threshold (now == acquired_until).
        # This should not yield any jobs.
        freezer.tick(30)
        assert not await datastore.acquire_jobs("worker2", 1)

        # Right after that, the job should be available
        freezer.tick(1)
        acquired = await datastore.acquire_jobs("worker2", 1)
        assert len(acquired) == 1
        assert acquired[0].id == job.id

    async def test_acquire_jobs_max_number_exceeded(
        self, datastore: AsyncDataStore
    ) -> None:
        await datastore.add_task(
            Task(id="task1", func=asynccontextmanager, max_running_jobs=2)
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
            acquired_jobs[0].task_id,
            JobResult.from_job(
                acquired_jobs[0],
                JobOutcome.success,
                return_value=None,
            ),
        )
        acquired_jobs = await datastore.acquire_jobs("worker1", 3)
        assert [job.id for job in acquired_jobs] == [jobs[2].id]

    async def test_add_get_task(self, datastore: DataStore) -> None:
        with pytest.raises(TaskLookupError):
            await datastore.get_task("dummyid")

        await datastore.add_task(Task(id="dummyid", func=asynccontextmanager))
        task = await datastore.get_task("dummyid")
        assert task.id == "dummyid"
        assert task.func is asynccontextmanager

    async def test_cancel_start(
        self, raw_datastore: AsyncDataStore, event_broker: AsyncEventBroker
    ) -> None:
        with CancelScope() as scope:
            scope.cancel()
            await raw_datastore.start(event_broker)
            await raw_datastore.stop()

    async def test_cancel_stop(
        self, raw_datastore: AsyncDataStore, event_broker: AsyncEventBroker
    ) -> None:
        with CancelScope() as scope:
            await raw_datastore.start(event_broker)
            scope.cancel()
            await raw_datastore.stop()
