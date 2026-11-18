from __future__ import annotations

from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from logging import getLogger
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock

import attrs
import pytest
from anyio import create_memory_object_stream, fail_after
from pymongo.asynchronous.mongo_client import AsyncMongoClient
from pymongo.errors import ConnectionFailure
from tenacity import wait_none
from test_rolling_upgrade import Child, Parent, invocation

from apscheduler import (
    AsyncScheduler,
    ConflictPolicy,
    DeserializationError,
    Job,
    JobOutcome,
    JobReleased,
    Schedule,
    ScheduleDeserializationFailed,
    ScheduleResult,
    SerializationError,
)
from apscheduler.abc import Serializer
from apscheduler.datastores.memory import MemoryDataStore
from apscheduler.datastores.mongodb import MongoDBDataStore, marshal_document
from apscheduler.datastores.sqlalchemy import SQLAlchemyDataStore
from apscheduler.triggers.date import DateTrigger

pytestmark = pytest.mark.anyio


class SerializingMemoryStore(MemoryDataStore):
    """An extension using memory indexes but round-tripping persistent records."""

    def __init__(self, serializer: Serializer) -> None:
        super().__init__()
        self.serializer = serializer

    async def add_job(self, job: Job) -> None:
        await super().add_job(
            Job.unmarshal(self.serializer, job.marshal(self.serializer))
        )

    async def add_schedule(
        self, schedule: Schedule, conflict_policy: ConflictPolicy
    ) -> None:
        await super().add_schedule(
            Schedule.unmarshal(self.serializer, schedule.marshal(self.serializer)),
            conflict_policy,
        )


@pytest.mark.parametrize("kind", ["job", "schedule"])
@pytest.mark.parametrize(
    "capability", [None, False, True], ids=["absent", "false", "true"]
)
async def test_memory_extension_requires_writer_opt_in(
    serializer: Serializer, kind: str, capability: bool | None
) -> None:
    store = SerializingMemoryStore(serializer)
    if capability is not None:
        store.allow_invocation_overrides = capability

    async with AsyncScheduler(store) as scheduler:
        configured = await scheduler.configure_task("rolling-upgrade", func=Parent.run)
        fire_time = datetime.now(UTC)
        trigger = DateTrigger(fire_time)

        async def submit() -> None:
            if kind == "job":
                await scheduler.add_job(Child.run, args=("user",))
            else:
                await scheduler.add_schedule(Child.run, trigger, args=("user",))

        if capability:
            await submit()
            records = (
                await scheduler.get_jobs()
                if kind == "job"
                else await scheduler.get_schedules()
            )
            assert len(records) == 1
            assert records[0]._func.endswith(":Child.run")
            assert records[0].args == ("user",)
        else:
            with pytest.raises(SerializationError, match="allow_invocation_overrides"):
                await submit()

            assert not await scheduler.get_jobs()
            assert not await scheduler.get_schedules()
            assert trigger.next() == fire_time

        [stored] = await scheduler.get_tasks()
        assert attrs.asdict(stored) == attrs.asdict(configured)


@pytest.fixture(params=["version", "shape", "bytes"])
def malformed_args(request: pytest.FixtureRequest, serializer: Serializer) -> bytes:
    if request.param == "bytes":
        return b"\xc1"

    envelope: dict[str, Any] = {
        "apscheduler:callable": 2,
        "func": "test:run",
        "args": [],
    }
    if request.param == "shape":
        envelope.update({"apscheduler:callable": 1, "args": 42})

    return serializer.serialize(envelope)


@pytest.mark.parametrize("update_returning", [True, False])
async def test_sql_schedule_acquisition_isolates_malformed_record(
    serializer: Serializer, malformed_args: bytes, update_returning: bool
) -> None:
    store = SQLAlchemyDataStore("sqlite+aiosqlite://", serializer=serializer)
    async with AsyncScheduler(store) as scheduler:
        store._supports_update_returning = update_returning
        bad = attrs.evolve(invocation("schedule"), id="bad")
        healthy = attrs.evolve(invocation("schedule"), id="healthy")
        await store.add_schedule(bad, ConflictPolicy.exception)
        await store.add_schedule(healthy, ConflictPolicy.exception)
        async with store._begin_transaction() as conn:
            await store._execute(
                conn,
                store._t_schedules.update()
                .where(store._t_schedules.c.id == "bad")
                .values(args=malformed_args),
            )

        send, receive = create_memory_object_stream[ScheduleDeserializationFailed](10)
        with (
            send,
            receive,
            scheduler.subscribe(send.send, {ScheduleDeserializationFailed}),
        ):
            before = datetime.now(UTC)
            acquired = await store.acquire_schedules(
                "worker", timedelta(seconds=30), 10
            )
            assert [schedule.id for schedule in acquired] == ["healthy"]
            with fail_after(5):
                failure = await receive.receive()

        assert failure.schedule_id == "bad"
        assert isinstance(failure.exception, DeserializationError)
        async with store._begin_transaction() as conn:
            rows = list(await store._execute(conn, store._t_schedules.select()))
        assert {row.id for row in rows} == {"bad", "healthy"}
        assert all(row.acquired_by == "worker" for row in rows)
        assert all(row.acquired_until >= before + timedelta(seconds=30) for row in rows)
        assert await store.acquire_schedules("other", timedelta(seconds=30), 10) == []
        await store.release_schedules(
            "worker",
            [
                ScheduleResult(
                    schedule_id=healthy.id,
                    task_id=healthy.task_id,
                    trigger=healthy.trigger,
                    last_fire_time=healthy.next_fire_time,
                    next_fire_time=None,
                )
            ],
        )


@pytest.fixture
def mongo_transport(serializer: Serializer) -> MongoDBDataStore:
    # Only the database transport is mocked; use real serializers, structures,
    # acquisition/release methods and retry handling. Never connect to a server.
    client = Mock(spec=AsyncMongoClient)
    client.get_database.return_value = {
        name: Mock() for name in ("tasks", "schedules", "jobs", "job_results")
    }
    client.start_session.side_effect = lambda: nullcontext(object())
    store = MongoDBDataStore(client, serializer=serializer)
    store._event_broker = Mock(publish=AsyncMock())
    store._logger = getLogger(__name__)
    store._schedules = Mock(update_many=AsyncMock())
    store._jobs = Mock(update_many=AsyncMock(), delete_one=AsyncMock())
    store._tasks = Mock(
        update_one=AsyncMock(return_value=SimpleNamespace(matched_count=1))
    )
    store._jobs_results = Mock(insert_one=AsyncMock())
    return store


def mongo_document(record: Job | Schedule, serializer: Serializer) -> dict[str, Any]:
    document = record.marshal(serializer)
    # PyMongo's real codecs convert timedeltas to seconds and enums to names.
    for key, value in document.items():
        if isinstance(value, timedelta):
            document[key] = value.total_seconds()
    marshal_document(document)
    return document


@pytest.mark.parametrize("bad_first", [True, False])
async def test_mongo_schedule_acquisition_isolates_malformed_record(
    mongo_transport: MongoDBDataStore, malformed_args: bytes, bad_first: bool
) -> None:
    store = mongo_transport
    records = [
        attrs.evolve(invocation("schedule"), id=id_) for id_ in ("bad", "healthy")
    ]
    documents = [mongo_document(record, store.serializer) for record in records]
    documents[0]["args"] = malformed_args
    if not bad_first:
        documents.reverse()
    store._schedules.find.return_value.to_list = AsyncMock(return_value=documents)
    store._schedules.update_many.return_value = SimpleNamespace(modified_count=2)

    before = datetime.now(UTC)
    acquired = await store.acquire_schedules("worker", timedelta(seconds=30), 10)
    assert [schedule.id for schedule in acquired] == ["healthy"]
    [call] = store._schedules.update_many.call_args_list
    assert set(call.args[0]["_id"]["$in"]) == {"bad", "healthy"}
    # Failed records retain their finite lease, matching SQL's failure policy;
    # they are reported for repair rather than deleted.
    assert call.args[1]["$set"]["acquired_by"] == "worker"
    assert call.args[1]["$set"]["acquired_until"] >= before.timestamp() + 30
    [call] = store._event_broker.publish.call_args_list
    event = call.args[0]
    assert isinstance(event, ScheduleDeserializationFailed)
    assert event.schedule_id == "bad"
    assert isinstance(event.exception, DeserializationError)


@pytest.mark.parametrize("owned_id", ["bad", "healthy"])
async def test_mongo_schedule_acquisition_only_processes_owned_records(
    mongo_transport: MongoDBDataStore, owned_id: str
) -> None:
    store = mongo_transport
    records = [
        attrs.evolve(invocation("schedule"), id=id_) for id_ in ("bad", "healthy")
    ]
    documents = [mongo_document(record, store.serializer) for record in records]
    documents[0]["args"] = store.serializer.serialize(
        {"apscheduler:callable": 2, "func": "test:run", "args": []}
    )
    cursor = Mock(to_list=AsyncMock(return_value=documents))
    owned_cursor = AsyncMock()
    owned_cursor.__aenter__.return_value.__aiter__.return_value = [{"_id": owned_id}]
    store._schedules.find.side_effect = [cursor, owned_cursor]
    store._schedules.update_many.return_value = SimpleNamespace(modified_count=1)

    acquired = await store.acquire_schedules("worker", timedelta(seconds=30), 10)
    if owned_id == "healthy":
        assert [schedule.id for schedule in acquired] == ["healthy"]
        store._event_broker.publish.assert_not_awaited()
    else:
        assert acquired == []
        [call] = store._event_broker.publish.call_args_list
        assert isinstance(call.args[0], ScheduleDeserializationFailed)
        assert call.args[0].schedule_id == "bad"

    ownership_filter = store._schedules.find.call_args.args[0]
    assert ownership_filter["acquired_by"] == "worker"
    assert set(ownership_filter["_id"]["$in"]) == {"bad", "healthy"}


@pytest.mark.parametrize("retryable", [False, True])
async def test_mongo_schedule_acquisition_does_not_suppress_transport_errors(
    mongo_transport: MongoDBDataStore, retryable: bool
) -> None:
    store = mongo_transport
    store.retry_settings = attrs.evolve(store.retry_settings, wait=wait_none())
    error = (
        ConnectionFailure("offline") if retryable else ValueError("broken transport")
    )
    cursor = store._schedules.find.return_value
    cursor.to_list = AsyncMock(side_effect=[error, []])
    if retryable:
        assert await store.acquire_schedules("worker", timedelta(seconds=30), 10) == []
        assert cursor.to_list.await_count == 2
    else:
        with pytest.raises(ValueError, match="broken transport"):
            await store.acquire_schedules("worker", timedelta(seconds=30), 10)


async def test_mongo_malformed_job_outcome_and_healthy_acquisition(
    mongo_transport: MongoDBDataStore, malformed_args: bytes
) -> None:
    store = mongo_transport
    bad = invocation("job", result_expiration_time=60)
    healthy = invocation("job")
    documents = [mongo_document(record, store.serializer) for record in (bad, healthy)]
    documents[0]["args"] = malformed_args
    store._jobs.find.return_value.to_list = AsyncMock(return_value=documents)
    store._jobs.update_many.return_value = SimpleNamespace(modified_count=2)

    acquired = await store.acquire_jobs("worker", timedelta(seconds=30), 10)
    assert [job.id for job in acquired] == [healthy.id]
    [call] = store._jobs_results.insert_one.call_args_list
    assert call.args[0]["outcome"] is JobOutcome.deserialization_failed
    store._jobs.delete_one.assert_awaited_once()
    assert store._jobs.delete_one.call_args.args[0] == {"_id": bad.id}
    store._tasks.update_one.assert_awaited_once()
    events = [call.args[0] for call in store._event_broker.publish.call_args_list]
    [released] = [event for event in events if isinstance(event, JobReleased)]
    assert released.job_id == bad.id
    assert released.outcome is JobOutcome.deserialization_failed
