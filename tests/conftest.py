from __future__ import annotations

import logging
from collections.abc import AsyncGenerator, Generator
from contextlib import AsyncExitStack
from logging import Logger
from pathlib import Path
from typing import Any, cast
from zoneinfo import ZoneInfo

import pytest
from _pytest.fixtures import SubRequest
from pytest_lazy_fixtures import lf

from apscheduler.abc import DataStore, EventBroker, Serializer
from apscheduler.datastores.memory import MemoryDataStore
from apscheduler.serializers.cbor import CBORSerializer
from apscheduler.serializers.json import JSONSerializer
from apscheduler.serializers.pickle import PickleSerializer


@pytest.fixture(scope="session")
def timezone() -> ZoneInfo:
    return ZoneInfo("Europe/Berlin")


@pytest.fixture(scope="session")
def utc_timezone() -> ZoneInfo:
    return ZoneInfo("UTC")


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

    broker = RedisEventBroker(
        "redis://localhost:6379", serializer=serializer, stop_check_interval=0.05
    )
    await broker._client.flushdb()
    return broker


@pytest.fixture
def mqtt_broker(serializer: Serializer) -> EventBroker:
    from apscheduler.eventbrokers.mqtt import MQTTEventBroker

    return MQTTEventBroker(serializer=serializer)


@pytest.fixture
async def asyncpg_broker(serializer: Serializer) -> EventBroker:
    pytest.importorskip("asyncpg", reason="asyncpg is not installed")
    from apscheduler.eventbrokers.asyncpg import AsyncpgEventBroker

    broker = AsyncpgEventBroker(
        "postgres://postgres:secret@localhost:5432/testdb", serializer=serializer
    )
    return broker


@pytest.fixture
async def psycopg_broker(serializer: Serializer) -> EventBroker:
    pytest.importorskip("psycopg", reason="psycopg is not installed")
    from apscheduler.eventbrokers.psycopg import PsycopgEventBroker

    broker = PsycopgEventBroker(
        "postgres://postgres:secret@localhost:5432/testdb", serializer=serializer
    )
    return broker


@pytest.fixture(
    params=[
        pytest.param(lf("local_broker"), id="local"),
        pytest.param(
            lf("asyncpg_broker"),
            id="asyncpg",
            marks=[pytest.mark.external_service],
        ),
        pytest.param(
            lf("psycopg_broker"),
            id="psycopg",
            marks=[pytest.mark.external_service],
        ),
        pytest.param(
            lf("redis_broker"),
            id="redis",
            marks=[pytest.mark.external_service],
        ),
        pytest.param(
            lf("mqtt_broker"), id="mqtt", marks=[pytest.mark.external_service]
        ),
    ]
)
async def raw_event_broker(request: SubRequest) -> EventBroker:
    return cast(EventBroker, request.param)


@pytest.fixture
async def event_broker(
    raw_event_broker: EventBroker, logger: Logger
) -> AsyncGenerator[EventBroker, Any]:
    async with AsyncExitStack() as exit_stack:
        await raw_event_broker.start(exit_stack, logger)
        yield raw_event_broker


@pytest.fixture
def memory_store() -> Generator[DataStore, None, None]:
    yield MemoryDataStore()


@pytest.fixture
def mongodb_store() -> Generator[DataStore, None, None]:
    from pymongo import MongoClient

    from apscheduler.datastores.mongodb import MongoDBDataStore

    client: MongoClient
    with MongoClient(tz_aware=True, serverSelectionTimeoutMS=1000) as client:
        yield MongoDBDataStore(client, start_from_scratch=True)


@pytest.fixture
async def psycopg_async_store() -> AsyncGenerator[DataStore, None]:
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    from apscheduler.datastores.sqlalchemy import SQLAlchemyDataStore

    engine = create_async_engine(
        "postgresql+psycopg://postgres:secret@localhost/testdb"
    )
    try:
        async with engine.begin() as conn:
            await conn.execute(text("CREATE SCHEMA IF NOT EXISTS psycopg_async"))

        yield SQLAlchemyDataStore(
            engine, schema="psycopg_async", start_from_scratch=True
        )
        assert "Current Checked out connections: 0" in engine.pool.status()
    finally:
        await engine.dispose()


@pytest.fixture
def psycopg_sync_store() -> Generator[DataStore, None, None]:
    from sqlalchemy import create_engine, text

    from apscheduler.datastores.sqlalchemy import SQLAlchemyDataStore

    engine = create_engine("postgresql+psycopg://postgres:secret@localhost/testdb")
    try:
        with engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS psycopg_sync"))

        yield SQLAlchemyDataStore(
            engine, schema="psycopg_sync", start_from_scratch=True
        )
        assert "Current Checked out connections: 0" in engine.pool.status()
    finally:
        engine.dispose()


@pytest.fixture
def pymysql_store() -> Generator[DataStore, None, None]:
    from sqlalchemy import create_engine

    from apscheduler.datastores.sqlalchemy import SQLAlchemyDataStore

    engine = create_engine("mysql+pymysql://root:secret@localhost/testdb")
    try:
        yield SQLAlchemyDataStore(engine, start_from_scratch=True)
        assert "Current Checked out connections: 0" in engine.pool.status()
    finally:
        engine.dispose()


@pytest.fixture
async def aiosqlite_store(tmp_path: Path) -> AsyncGenerator[DataStore, None]:
    from sqlalchemy.ext.asyncio import create_async_engine

    from apscheduler.datastores.sqlalchemy import SQLAlchemyDataStore

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/test.db")
    try:
        yield SQLAlchemyDataStore(engine)
    finally:
        await engine.dispose()


@pytest.fixture
async def asyncpg_store() -> AsyncGenerator[DataStore, None]:
    pytest.importorskip("asyncpg", reason="asyncpg is not installed")
    from asyncpg import compat
    from sqlalchemy.ext.asyncio import create_async_engine

    from apscheduler.datastores.sqlalchemy import SQLAlchemyDataStore

    # Workaround for AnyIO 4.0.0rc1 compatibility
    async def patched_wait_for(fut, timeout):
        import asyncio

        if timeout is None:
            return await fut
        fut = asyncio.ensure_future(fut)
        try:
            return await asyncio.wait_for(fut, timeout)
        except asyncio.CancelledError:
            if fut.done() and not fut.cancelled():
                return fut.result()
            else:
                raise

    compat.wait_for = patched_wait_for

    engine = create_async_engine(
        "postgresql+asyncpg://postgres:secret@localhost/testdb", future=True
    )
    try:
        yield SQLAlchemyDataStore(engine, start_from_scratch=True)
        assert "Current Checked out connections: 0" in engine.pool.status()
    finally:
        await engine.dispose()


@pytest.fixture
async def asyncmy_store() -> AsyncGenerator[DataStore, None]:
    pytest.importorskip("asyncmy", reason="asyncmy is not installed")
    from sqlalchemy.ext.asyncio import create_async_engine

    from apscheduler.datastores.sqlalchemy import SQLAlchemyDataStore

    engine = create_async_engine(
        "mysql+asyncmy://root:secret@localhost/testdb?charset=utf8mb4", future=True
    )
    try:
        yield SQLAlchemyDataStore(engine, start_from_scratch=True)
        assert "Current Checked out connections: 0" in engine.pool.status()
    finally:
        await engine.dispose()


@pytest.fixture(
    params=[
        pytest.param(
            lf("memory_store"),
            id="memory",
        ),
        pytest.param(
            lf("aiosqlite_store"),
            id="aiosqlite",
        ),
        pytest.param(
            lf("asyncpg_store"),
            id="asyncpg",
            marks=[pytest.mark.external_service],
        ),
        pytest.param(
            lf("asyncmy_store"),
            id="asyncmy",
            marks=[pytest.mark.external_service],
        ),
        pytest.param(
            lf("psycopg_async_store"),
            id="psycopg_async",
            marks=[pytest.mark.external_service],
        ),
        pytest.param(
            lf("psycopg_sync_store"),
            id="psycopg_sync",
            marks=[pytest.mark.external_service],
        ),
        pytest.param(
            lf("pymysql_store"),
            id="pymysql",
            marks=[pytest.mark.external_service],
        ),
        pytest.param(
            lf("mongodb_store"),
            id="mongodb",
            marks=[pytest.mark.external_service],
        ),
    ]
)
async def raw_datastore(request: SubRequest) -> DataStore:
    return cast(DataStore, request.param)


@pytest.fixture(scope="session")
def logger() -> Logger:
    return logging.getLogger("apscheduler")
