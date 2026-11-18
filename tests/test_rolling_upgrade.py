from __future__ import annotations

import sys
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from functools import partial
from pathlib import Path
from types import ModuleType
from typing import Any

import attrs
import pytest
from anyio import create_memory_object_stream, fail_after

from apscheduler import (
    AsyncScheduler,
    ConflictPolicy,
    Job,
    JobOutcome,
    JobReleased,
    JobResult,
    Schedule,
    Scheduler,
    SerializationError,
    task,
)
from apscheduler._marshalling import callable_from_ref, callable_to_ref
from apscheduler.abc import DataStore, Serializer
from apscheduler.datastores.memory import MemoryDataStore
from apscheduler.datastores.mongodb import MongoDBDataStore
from apscheduler.datastores.sqlalchemy import SQLAlchemyDataStore
from apscheduler.triggers.date import DateTrigger

pytestmark = pytest.mark.anyio


class Parent:
    @classmethod
    @task(id="rolling-upgrade")
    def run(cls, *args: Any, **kwargs: Any) -> list:
        # Permissive signatures expose silent wrong-class execution by old readers.
        return [cls.__name__, list(args), kwargs]


class Child(Parent):
    pass


@pytest.fixture
def old_structures(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    # Exact source blob 94074b3ef4dfa4e43409e22f75857dfdc69fab07, shared by
    # base bdaceb12792f104f25798dee2850d45f9e7d83f8 and prior public head
    # 656cc5bd66c98e21a059c2568fae9c0d08484391. No reader logic is emulated.
    name = "apscheduler._rolling_upgrade_old_structures"
    module = ModuleType(name)
    module.__package__ = "apscheduler"
    monkeypatch.setitem(sys.modules, name, module)
    source = Path(__file__).parent / "compat" / "structures_94074b3.py.txt"
    exec(compile(source.read_bytes(), str(source), "exec"), module.__dict__)
    return module


def invocation(kind: str, **kwargs: Any) -> Job | Schedule:
    if kind == "job":
        return Job(task_id="rolling-upgrade", executor="async", **kwargs)

    trigger = DateTrigger(datetime.now(UTC))
    next_fire_time = trigger.next()
    return Schedule(
        id="rolling-upgrade",
        task_id="rolling-upgrade",
        job_executor="async",
        trigger=trigger,
        next_fire_time=next_fire_time,
        **kwargs,
    )


@pytest.mark.parametrize("kind", ["job", "schedule"])
async def test_actual_old_reader_silently_accepts_envelope(
    old_structures: ModuleType, serializer: Serializer, kind: str
) -> None:
    record = invocation(kind, func=callable_to_ref(Child.run), args=("user",))
    wire = record.marshal(serializer)
    reader = old_structures.Job if kind == "job" else old_structures.Schedule
    old = reader.unmarshal(serializer, wire.copy())
    assert old.args == ("apscheduler:callable", "func", "args")
    assert Parent.run(*old.args) == [
        "Parent",
        ["apscheduler:callable", "func", "args"],
        {},
    ]
    new = type(record).unmarshal(serializer, wire.copy())
    assert callable_from_ref(new._func)(*new.args) == ["Child", ["user"], {}]


@pytest.mark.parametrize("kind", ["job", "schedule"])
async def test_actual_old_writer_is_readable_without_reinterpreting_user_args(
    old_structures: ModuleType, serializer: Serializer, kind: str
) -> None:
    args = ({"apscheduler:callable": 1, "func": "user:data", "args": []}, "user")
    if kind == "job":
        record = old_structures.Job(
            task_id="rolling-upgrade", executor="async", args=args
        )
        reader = Job
    else:
        record = old_structures.Schedule(
            id="rolling-upgrade",
            task_id="rolling-upgrade",
            job_executor="async",
            trigger=DateTrigger(datetime.now(UTC)),
            args=args,
        )
        reader = Schedule

    restored = reader.unmarshal(serializer, record.marshal(serializer))
    assert restored._func is None
    assert restored.args == args


@pytest.mark.parametrize("kind", ["job", "schedule"])
@pytest.mark.parametrize("use_partial", [False, True])
async def test_reader_stage_rejects_before_task_changes(
    raw_datastore: DataStore, kind: str, use_partial: bool
) -> None:
    if isinstance(raw_datastore, MemoryDataStore):
        pytest.skip("MemoryDataStore does not serialize records")

    async with AsyncScheduler(raw_datastore) as scheduler:
        configured = await scheduler.configure_task(
            "rolling-upgrade",
            func=Parent.run,
            max_running_jobs=3,
            misfire_grace_time=42,
            metadata={"configured": True},
        )
        target = partial(Child.run, "user") if use_partial else Child.run
        fire_time = datetime.now(UTC)
        trigger = DateTrigger(fire_time)
        with pytest.raises(SerializationError, match="allow_invocation_overrides"):
            if kind == "job":
                await scheduler.add_job(target)
            else:
                await scheduler.add_schedule(target, trigger)

        assert trigger.next() == fire_time
        assert not await scheduler.get_jobs()
        assert not await scheduler.get_schedules()
        [stored] = await scheduler.get_tasks()
        assert attrs.asdict(stored) == attrs.asdict(configured)


@pytest.mark.parametrize("kind", ["job", "schedule"])
async def test_reader_stage_direct_store_writes_fail_closed(
    raw_datastore: DataStore, kind: str
) -> None:
    if isinstance(raw_datastore, MemoryDataStore):
        pytest.skip("MemoryDataStore does not serialize records")

    async with AsyncScheduler(raw_datastore) as scheduler:
        marked = invocation(kind, func=callable_to_ref(Child.run))
        with pytest.raises(SerializationError, match="allow_invocation_overrides"):
            if isinstance(marked, Job):
                await scheduler.data_store.add_job(marked)
            else:
                await scheduler.data_store.add_schedule(
                    marked, ConflictPolicy.exception
                )

        assert not await scheduler.get_jobs()
        assert not await scheduler.get_schedules()


@pytest.mark.parametrize("kind", ["job", "schedule"])
async def test_reader_stage_shared_sqlite_store_with_actual_old_readers(
    tmp_path: Path,
    serializer: Serializer,
    old_structures: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    from apscheduler.datastores import sqlalchemy as store_module

    url = f"sqlite+aiosqlite:///{tmp_path}/shared.db"
    async with AsyncScheduler(
        SQLAlchemyDataStore(url, serializer=serializer)
    ) as producer:
        configured = await producer.configure_task(
            "rolling-upgrade",
            func=Parent.run,
            max_running_jobs=2,
            misfire_grace_time=42,
            metadata={"configured": True},
        )
        # An ID submission has the legacy meaning. The failed callable submission
        # cannot replace its task binding or leave an envelope for an old reader.
        if kind == "job":
            await producer.add_job("rolling-upgrade", args=("user",))
            with pytest.raises(SerializationError):
                await producer.add_job(Child.run, args=("wrong",))
        else:
            await producer.add_schedule(
                "rolling-upgrade", DateTrigger(datetime.now(UTC)), args=("user",)
            )
            with pytest.raises(SerializationError):
                await producer.add_schedule(Child.run, DateTrigger(datetime.now(UTC)))

    # Separate store/engine, acquired through the real SQL store path, with the
    # historical structure readers. Sequential SQLite access tests wire behavior;
    # it does not assert that SQLite supports concurrent multi-scheduler deployments.
    with monkeypatch.context() as patch:
        patch.setattr(store_module, "Job", old_structures.Job)
        patch.setattr(store_module, "Schedule", old_structures.Schedule)
        async with AsyncScheduler(
            SQLAlchemyDataStore(url, serializer=serializer)
        ) as old:
            if kind == "schedule":
                [schedule] = await old.data_store.acquire_schedules(
                    "old", timedelta(seconds=30), 10
                )
                assert schedule.args == ("user",)
                await old.data_store.add_job(
                    old_structures.Job(
                        task_id=schedule.task_id,
                        executor=schedule.job_executor,
                        args=schedule.args,
                        kwargs=schedule.kwargs,
                    )
                )

            [job] = await old.data_store.acquire_jobs("old", timedelta(seconds=30), 10)
            assert job.args == ("user",)
            stored = await old.data_store.get_task(job.task_id)
            assert stored.func == configured.func
            assert callable_from_ref(stored.func)(*job.args, **job.kwargs) == [
                "Parent",
                ["user"],
                {},
            ]
            await old.data_store.release_job(
                "old", job, JobResult.from_job(job, JobOutcome.success)
            )
            stored = await old.data_store.get_task(job.task_id)
            assert attrs.asdict(stored) == attrs.asdict(configured)


@pytest.mark.parametrize("kind", ["job", "schedule"])
async def test_opt_in_then_reader_stage_can_drain_existing_records(
    tmp_path: Path,
    serializer: Serializer,
    kind: str,
) -> None:
    url = f"sqlite+aiosqlite:///{tmp_path}/stages.db"
    store = SQLAlchemyDataStore(
        url, serializer=serializer, allow_invocation_overrides=True
    )
    async with AsyncScheduler(store) as producer:
        await producer.configure_task("rolling-upgrade", func=Parent.run)
        if kind == "job":
            await producer.add_job(Child.run, args=("user",), result_expiration_time=60)
        else:
            await producer.add_schedule(
                Child.run,
                DateTrigger(datetime.now(UTC)),
                args=("user",),
                job_result_expiration_time=60,
            )

    # A reader-stage worker can still execute marked jobs. A scheduler that
    # materializes marked schedules must keep the writer opt-in until drained.
    store = SQLAlchemyDataStore(
        url, serializer=serializer, allow_invocation_overrides=(kind == "schedule")
    )
    async with AsyncScheduler(store) as reader:
        send, receive = create_memory_object_stream[JobReleased](1)
        with send, receive, reader.subscribe(send.send, {JobReleased}):
            await reader.start_in_background()
            with fail_after(5):
                event = await receive.receive()
        result = await reader.get_job_result(event.job_id)
        assert result and result.outcome is JobOutcome.success
        assert result.return_value == ["Child", ["user"], {}]
        [configured] = await reader.get_tasks()
        assert configured.func == callable_to_ref(Parent.run)
        assert configured.running_jobs == 0


def test_persistent_writer_opt_in_is_explicit() -> None:
    assert (
        SQLAlchemyDataStore("sqlite+aiosqlite://").allow_invocation_overrides is False
    )
    assert MongoDBDataStore("mongodb://localhost").allow_invocation_overrides is False


@pytest.mark.parametrize("kind", ["job", "schedule"])
@pytest.mark.parametrize(
    "factory",
    [
        lambda: SQLAlchemyDataStore("sqlite+aiosqlite://"),
        lambda: MongoDBDataStore("mongodb://localhost"),
    ],
    ids=["sqlalchemy", "mongodb"],
)
async def test_store_guard_runs_before_database_access(
    kind: str, factory: Callable[[], DataStore]
) -> None:
    # These stores have never been started. A write that reached the database
    # would fail or try to connect; the admission check must run first.
    store = factory()
    record = invocation(kind, func=callable_to_ref(Child.run))
    with pytest.raises(SerializationError, match="allow_invocation_overrides"):
        if isinstance(record, Job):
            await store.add_job(record)
        else:
            await store.add_schedule(record, ConflictPolicy.exception)


@pytest.mark.parametrize("kind", ["job", "schedule"])
def test_sync_reader_stage_rejects_without_creating_task(
    tmp_path: Path, kind: str
) -> None:
    store = SQLAlchemyDataStore(f"sqlite+aiosqlite:///{tmp_path}/sync.db")
    with Scheduler(store) as scheduler:
        with pytest.raises(SerializationError, match="allow_invocation_overrides"):
            if kind == "job":
                scheduler.add_job(Child.run)
            else:
                scheduler.add_schedule(Child.run, DateTrigger(datetime.now(UTC)))

        assert not scheduler.get_tasks()
        assert not scheduler.get_jobs()
        assert not scheduler.get_schedules()
