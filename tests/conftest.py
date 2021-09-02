import sys
from contextlib import asynccontextmanager, contextmanager
from tempfile import TemporaryDirectory
from typing import AsyncContextManager, AsyncGenerator, ContextManager, Generator, Optional

import pytest

from apscheduler.abc import AsyncDataStore, DataStore, Serializer
from apscheduler.datastores.async_.sync_adapter import AsyncDataStoreAdapter
from apscheduler.datastores.sync.memory import MemoryDataStore
from apscheduler.serializers.cbor import CBORSerializer
from apscheduler.serializers.json import JSONSerializer
from apscheduler.serializers.pickle import PickleSerializer

if sys.version_info >= (3, 9):
    from zoneinfo import ZoneInfo
else:
    from backports.zoneinfo import ZoneInfo


@pytest.fixture(scope='session')
def timezone() -> ZoneInfo:
    return ZoneInfo('Europe/Berlin')


@pytest.fixture(params=[
    pytest.param(None, id='none'),
    pytest.param(PickleSerializer, id='pickle'),
    pytest.param(CBORSerializer, id='cbor'),
    pytest.param(JSONSerializer, id='json')
])
def serializer(request) -> Optional[Serializer]:
    return request.param() if request.param else None


@pytest.fixture
def anyio_backend() -> 'str':
    return 'asyncio'


@contextmanager
def setup_memory_store() -> Generator[DataStore, None, None]:
    yield MemoryDataStore()


@contextmanager
def setup_mongodb_store() -> Generator[DataStore, None, None]:
    from pymongo import MongoClient

    from apscheduler.datastores.sync.mongodb import MongoDBDataStore

    with MongoClient(tz_aware=True, serverSelectionTimeoutMS=1000) as client:
        yield MongoDBDataStore(client, start_from_scratch=True)


@contextmanager
def setup_sqlite_store() -> Generator[DataStore, None, None]:
    from sqlalchemy.future import create_engine

    from apscheduler.datastores.sync.sqlalchemy import SQLAlchemyDataStore

    with TemporaryDirectory('sqlite_') as tempdir:
        engine = create_engine(f'sqlite:///{tempdir}/test.db')
        try:
            yield SQLAlchemyDataStore(engine)
        finally:
            engine.dispose()


@contextmanager
def setup_psycopg2_store() -> Generator[DataStore, None, None]:
    from sqlalchemy.future import create_engine

    from apscheduler.datastores.sync.sqlalchemy import SQLAlchemyDataStore

    engine = create_engine('postgresql+psycopg2://postgres:secret@localhost/testdb')
    try:
        yield SQLAlchemyDataStore(engine, start_from_scratch=True)
    finally:
        engine.dispose()


@contextmanager
def setup_mysql_store() -> Generator[DataStore, None, None]:
    from sqlalchemy.future import create_engine

    from apscheduler.datastores.sync.sqlalchemy import SQLAlchemyDataStore

    engine = create_engine('mysql+pymysql://root:secret@localhost/testdb')
    try:
        yield SQLAlchemyDataStore(engine, start_from_scratch=True)
    finally:
        engine.dispose()


@asynccontextmanager
async def setup_asyncpg_store() -> AsyncGenerator[AsyncDataStore, None]:
    from sqlalchemy.ext.asyncio import create_async_engine

    from apscheduler.datastores.async_.sqlalchemy import SQLAlchemyDataStore

    engine = create_async_engine('postgresql+asyncpg://postgres:secret@localhost/testdb',
                                 future=True)
    try:
        yield SQLAlchemyDataStore(engine, start_from_scratch=True)
    finally:
        await engine.dispose()


@pytest.fixture(params=[
    pytest.param(setup_memory_store, id='memory'),
    pytest.param(setup_sqlite_store, id='sqlite'),
    pytest.param(setup_mongodb_store, id='mongodb', marks=[pytest.mark.externaldb]),
    pytest.param(setup_psycopg2_store, id='psycopg2', marks=[pytest.mark.externaldb]),
    pytest.param(setup_mysql_store, id='mysql', marks=[pytest.mark.externaldb])
])
def setup_sync_store(request) -> ContextManager[DataStore]:
    return request.param


@pytest.fixture(params=[
    pytest.param(setup_asyncpg_store, id='asyncpg', marks=[pytest.mark.externaldb])
])
def setup_async_store(request) -> AsyncContextManager[AsyncDataStore]:
    return request.param


@pytest.fixture(params=[
    pytest.param(setup_memory_store, id='memory'),
    pytest.param(setup_sqlite_store, id='sqlite'),
    pytest.param(setup_mongodb_store, id='mongodb', marks=[pytest.mark.externaldb]),
    pytest.param(setup_psycopg2_store, id='psycopg2', marks=[pytest.mark.externaldb]),
    pytest.param(setup_mysql_store, id='mysql', marks=[pytest.mark.externaldb]),
    pytest.param(setup_asyncpg_store, id='asyncpg', marks=[pytest.mark.externaldb])
])
async def datastore_cm(request):
    cm = request.param()
    if isinstance(cm, ContextManager):
        with cm as store:
            yield AsyncDataStoreAdapter(store)
    else:
        async with cm as store:
            yield store
