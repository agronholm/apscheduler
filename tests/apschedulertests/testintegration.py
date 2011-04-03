from datetime import datetime, timedelta
from time import sleep
from warnings import filterwarnings, resetwarnings
from tempfile import NamedTemporaryFile
import os

from nose.tools import eq_, raises
from nose.plugins.skip import SkipTest

from apscheduler.scheduler import Scheduler

try:
    from apscheduler.jobstores.shelve_store import ShelveJobStore
except ImportError:
    ShelveJobStore = None

try:
    from apscheduler.jobstores.sqlalchemy_store import SQLAlchemyJobStore
except ImportError:
    SQLAlchemyJobStore = None

try:
    from apscheduler.jobstores.mongodb_store import MongoDBJobStore
except ImportError:
    MongoDBJobStore = None


def increment(vals):
    vals[0] += 1
    sleep(2)


def renameTestMethods(cls, name):
    for attr in dir(cls):
        if attr.startswith('test_'):
            method = getattr(cls, attr)
            if hasattr(method, 'im_func'):
                method = method.im_func
            method.__doc__ = '%s/%s' % (name, attr)


class IntegrationTestBase(object):
    @classmethod
    def setup_class(cls):
        cls.jobstore = cls.make_jobstore()
        cls.scheduler = Scheduler()
        cls.scheduler.add_jobstore(cls.jobstore, 'persistent')
        cls.scheduler.start()

    def test_overlapping_runs(self):
        # Makes sure that "increment" is only ran once, since it will still be
        # running when the next appointed time hits.

        vals = [0]
        self.scheduler.add_interval_job(increment, jobstore='persistent',
                                        seconds=1, args=[vals])
        sleep(2.2)
        eq_(vals, [1])


class TestShelveIntegration(IntegrationTestBase):
    @staticmethod
    def make_jobstore():
        if not ShelveJobStore:
            raise SkipTest

        filterwarnings('ignore', category=RuntimeWarning)
        f = NamedTemporaryFile(prefix='apscheduler_')
        f.close()
        resetwarnings()
        return ShelveJobStore(f.name)

    @classmethod
    def teardown_class(cls):
        cls.scheduler.shutdown()
        cls.jobstore.close()
        if os.path.exists(cls.jobstore.path):
            os.remove(cls.jobstore.path)


class TestSQLAlchemyIntegration(IntegrationTestBase):
    @staticmethod
    def make_jobstore():
        if not SQLAlchemyJobStore:
            raise SkipTest

        return SQLAlchemyJobStore(url='sqlite:///example.sqlite')

    @classmethod
    def teardown_class(cls):
        cls.scheduler.shutdown()
        cls.jobstore.close()
        if os.path.exists('example.sqlite'):
            os.remove('example.sqlite')

renameTestMethods(TestSQLAlchemyIntegration, 'SQLAlchemy')


class TestMongoDBIntegration(IntegrationTestBase):
    @staticmethod
    def make_jobstore():
        if not MongoDBJobStore:
            raise SkipTest

        return MongoDBJobStore(database='apscheduler_unittest')

    @classmethod
    def teardown_class(cls):
        cls.scheduler.shutdown()
        connection = cls.jobstore.collection.database.connection
        connection.drop_database(cls.jobstore.collection.database.name)
        cls.jobstore.close()

renameTestMethods(TestMongoDBIntegration, 'MongoDB')
