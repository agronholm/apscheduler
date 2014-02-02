from warnings import filterwarnings, resetwarnings
from tempfile import NamedTemporaryFile
import sys
import os

import pytest

from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.jobstores.shelve import ShelveJobStore
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.jobstores.redis import RedisJobStore


@pytest.fixture
def memjobstore(request):
    return MemoryJobStore()


@pytest.fixture
def shelvejobstore(request):
    def finish():
        store.close()
        if os.path.exists(store.path):
            os.remove(store.path)

    filterwarnings('ignore', category=RuntimeWarning)
    f = NamedTemporaryFile(prefix='apscheduler_')
    f.close()
    resetwarnings()
    store = ShelveJobStore(f.name)
    request.addfinalizer(finish)
    return store


@pytest.fixture
def sqlalchemyjobstore(request):
    def finish():
        store.close()
        os.remove('tempdb.sqlite')

    if not SQLAlchemyJobStore:
        pytest.skip('SQLAlchemyJobStore missing')

    store = SQLAlchemyJobStore(url='sqlite:///tempdb.sqlite')
    request.addfinalizer(finish)
    return store


@pytest.fixture
def mongodbjobstore(request):
    def finish():
        connection = store.collection.database.connection
        connection.drop_database(store.collection.database.name)
        store.close()

    if not MongoDBJobStore:
        pytest.skip('MongoDBJobStore missing')

    store = MongoDBJobStore(database='apscheduler_unittest')
    request.addfinalizer(finish)
    return store


@pytest.fixture
def redisjobstore(request):
    def finish():
        store.redis.flushdb()
        store.close()

    if not RedisJobStore:
        pytest.skip('RedisJobStore missing')

    store = RedisJobStore()
    request.addfinalizer(finish)
    return store


@pytest.fixture
def jobstore(request):
    return request.param(request)


def minpython(*version):
    version_str = '.'.join([str(num) for num in version])

    def outer(func):
        dec = pytest.mark.skipif(sys.version_info < version,
                                 reason='This test requires at least Python %s' % version_str)
        return dec(func)
    return outer


all_jobstores = [memjobstore, shelvejobstore, sqlalchemyjobstore, mongodbjobstore, redisjobstore]
all_jobstores_ids = ['memory', 'shelve', 'sqlalchemy', 'mongodb', 'redis']
persistent_jobstores = [shelvejobstore, sqlalchemyjobstore, mongodbjobstore, redisjobstore]
persistent_jobstores_ids = ['shelve', 'sqlalchemy', 'mongodb', 'redis']
