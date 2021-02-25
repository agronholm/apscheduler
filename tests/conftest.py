import sys
from contextlib import AsyncExitStack, ExitStack
from functools import partial

import pytest
from anyio import start_blocking_portal
from apscheduler.datastores.memory import MemoryDataStore
from apscheduler.serializers.cbor import CBORSerializer
from apscheduler.serializers.json import JSONSerializer
from apscheduler.serializers.pickle import PickleSerializer
from asyncpg import create_pool
from motor.motor_asyncio import AsyncIOMotorClient

if sys.version_info >= (3, 9):
    from zoneinfo import ZoneInfo
else:
    from backports.zoneinfo import ZoneInfo

try:
    from apscheduler.datastores.mongodb import MongoDBDataStore
except ImportError:
    MongoDBDataStore = None

try:
    from apscheduler.datastores.postgresql import PostgresqlDataStore
except ImportError:
    PostgresqlDataStore = None

store_params = [
    pytest.param(MemoryDataStore, id='memory'),
    pytest.param(PostgresqlDataStore, id='postgresql'),
    pytest.param(MongoDBDataStore, id='mongodb')
]


@pytest.fixture(scope='session')
def timezone():
    return ZoneInfo('Europe/Berlin')


@pytest.fixture(params=[None, PickleSerializer, CBORSerializer, JSONSerializer],
                ids=['none', 'pickle', 'cbor', 'json'])
def serializer(request):
    return request.param() if request.param else None


@pytest.fixture
def anyio_backend():
    return 'asyncio'


@pytest.fixture(params=store_params)
async def store(request):
    async with AsyncExitStack() as stack:
        if request.param is PostgresqlDataStore:
            if PostgresqlDataStore is None:
                pytest.skip('asyncpg not installed')

            pool = await create_pool('postgresql://postgres:secret@localhost/testdb',
                                     min_size=1, max_size=2)
            await stack.enter_async_context(pool)
            store = PostgresqlDataStore(pool, start_from_scratch=True)
        elif request.param is MongoDBDataStore:
            if MongoDBDataStore is None:
                pytest.skip('motor not installed')

            client = AsyncIOMotorClient(tz_aware=True)
            stack.push(lambda *args: client.close())
            store = MongoDBDataStore(client, start_from_scratch=True)
        else:
            store = MemoryDataStore()

        await stack.enter_async_context(store)
        yield store


@pytest.fixture
def portal():
    with start_blocking_portal() as portal:
        yield portal


@pytest.fixture(params=store_params)
def sync_store(request, portal):
    with ExitStack() as stack:
        if request.param is PostgresqlDataStore:
            if PostgresqlDataStore is None:
                pytest.skip('asyncpg not installed')

            pool = portal.call(
                partial(create_pool, 'postgresql://postgres:secret@localhost/testdb',
                        min_size=1, max_size=2)
            )
            stack.enter_context(portal.wrap_async_context_manager(pool))
            store = PostgresqlDataStore(pool, start_from_scratch=True)
        elif request.param is MongoDBDataStore:
            if MongoDBDataStore is None:
                pytest.skip('motor not installed')

            client = portal.call(partial(AsyncIOMotorClient, tz_aware=True))
            stack.push(lambda *args: portal.call(client.close))
            store = MongoDBDataStore(client, start_from_scratch=True)
        else:
            store = MemoryDataStore()

        stack.enter_context(portal.wrap_async_context_manager(store))
        yield store
