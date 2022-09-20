from __future__ import annotations

import sys
from contextlib import AsyncExitStack
from tempfile import TemporaryDirectory
from typing import Any, AsyncGenerator, cast

import pytest
from _pytest.fixtures import SubRequest
from pytest_lazyfixture import lazy_fixture

from apscheduler.abc import DataStore, EventBroker, Serializer
from apscheduler.datastores.memory import MemoryDataStore
from apscheduler.serializers.cbor import CBORSerializer
from apscheduler.serializers.json import JSONSerializer
from apscheduler.serializers.pickle import PickleSerializer

if sys.version_info >= (3, 9):
    from zoneinfo import ZoneInfo
else:
    from backports.zoneinfo import ZoneInfo


@pytest.fixture(scope="session")
def timezone() -> ZoneInfo:
    return ZoneInfo("Europe/Berlin")


@pytest.fixture(
    params=[
        pytest.param(PickleSerializer, id="pickle"),
        pytest.param(CBORSerializer, id="cbor"),
        pytest.param(JSONSerializer, id="json"),
    ]
)
def serializer(request) -> Serializer | None:
    return request.param() if request.param else None


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
def local_broker() -> EventBroker:
    from apscheduler.eventbrokers.local import LocalEventBroker

    return LocalEventBroker()


@pytest.fixture
async def redis_broker(serializer: Serializer) -> EventBroker:
    from apscheduler.eventbrokers.redis import RedisEventBroker

    broker = RedisEventBroker.from_url(
        "redis://localhost:6379", serializer=serializer, stop_check_interval=0.05
    )
    await broker.client.flushdb()
    return broker


@pytest.fixture
def mqtt_broker(serializer: Serializer) -> EventBroker:
    from paho.mqtt.client import Client

    from apscheduler.eventbrokers.mqtt import MQTTEventBroker

    return MQTTEventBroker(Client(), serializer=serializer)


@pytest.fixture
async def asyncpg_broker(serializer: Serializer) -> EventBroker:
    from apscheduler.eventbrokers.asyncpg import AsyncpgEventBroker

    broker = AsyncpgEventBroker.from_dsn(
        "postgres://postgres:secret@localhost:5432/testdb", serializer=serializer
    )
    return broker


@pytest.fixture(
    params=[
        pytest.param(lazy_fixture("local_broker"), id="local"),
        pytest.param(
            lazy_fixture("asyncpg_broker"),
            id="asyncpg",
            marks=[pytest.mark.external_service],
        ),
        pytest.param(
            lazy_fixture("redis_broker"),
            id="redis",
            marks=[pytest.mark.external_service],
        ),
        pytest.param(
            lazy_fixture("mqtt_broker"), id="mqtt", marks=[pytest.mark.external_service]
        ),
    ]
)
async def raw_event_broker(request: SubRequest) -> EventBroker:
    return cast(EventBroker, request.param)


@pytest.fixture
async def event_broker(
    raw_event_broker: EventBroker,
) -> AsyncGenerator[EventBroker, Any]:
    async with AsyncExitStack() as exit_stack:
        await raw_event_broker.start(exit_stack)
        yield raw_event_broker


@pytest.fixture
def memory_store() -> DataStore:
    yield MemoryDataStore()


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
    from sqlalchemy import text
    from sqlalchemy.future import create_engine

    from apscheduler.datastores.sqlalchemy import SQLAlchemyDataStore

    engine = create_engine("postgresql+psycopg2://postgres:secret@localhost/testdb")
    try:
        with engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS psycopg2"))

        yield SQLAlchemyDataStore(engine, schema="psycopg2", start_from_scratch=True)
    finally:
        engine.dispose()


@pytest.fixture
def pymysql_store() -> DataStore:
    from sqlalchemy.future import create_engine

    from apscheduler.datastores.sqlalchemy import SQLAlchemyDataStore

    engine = create_engine("mysql+pymysql://root:secret@localhost/testdb")
    try:
        yield SQLAlchemyDataStore(engine, start_from_scratch=True)
    finally:
        engine.dispose()


@pytest.fixture
async def asyncpg_store() -> DataStore:
    from sqlalchemy.ext.asyncio import create_async_engine

    from apscheduler.datastores.sqlalchemy import SQLAlchemyDataStore

    engine = create_async_engine(
        "postgresql+asyncpg://postgres:secret@localhost/testdb", future=True
    )
    try:
        yield SQLAlchemyDataStore(engine, start_from_scratch=True)
    finally:
        await engine.dispose()


@pytest.fixture
async def asyncmy_store() -> DataStore:
    from sqlalchemy.ext.asyncio import create_async_engine

    from apscheduler.datastores.sqlalchemy import SQLAlchemyDataStore

    engine = create_async_engine(
        "mysql+asyncmy://root:secret@localhost/testdb?charset=utf8mb4", future=True
    )
    try:
        yield SQLAlchemyDataStore(engine, start_from_scratch=True)
    finally:
        await engine.dispose()


@pytest.fixture(
    params=[
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
        pytest.param(
            lazy_fixture("psycopg2_store"),
            id="psycopg2",
            marks=[pytest.mark.external_service],
        ),
        pytest.param(
            lazy_fixture("pymysql_store"),
            id="pymysql",
            marks=[pytest.mark.external_service],
        ),
        pytest.param(
            lazy_fixture("mongodb_store"),
            id="mongodb",
            marks=[pytest.mark.external_service],
        ),
    ]
)
async def raw_datastore(request: SubRequest) -> DataStore:
    return cast(DataStore, request.param)


@pytest.fixture
async def datastore(
    raw_datastore: DataStore, local_broker: EventBroker
) -> AsyncGenerator[DataStore, Any]:
    async with AsyncExitStack() as exit_stack:
        await local_broker.start(exit_stack)
        await raw_datastore.start(exit_stack, local_broker)
        yield raw_datastore
