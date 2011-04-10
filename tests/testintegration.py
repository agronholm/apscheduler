from time import sleep
from warnings import filterwarnings, resetwarnings
from tempfile import NamedTemporaryFile
import os

from nose.tools import eq_
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
        sleep(2.5)
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

    def test_overlapping_runs(self):
        """Shelve/test_overlapping_runs"""
        IntegrationTestBase.test_overlapping_runs(self)

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

    def test_overlapping_runs(self):
        """SQLAlchemy/test_overlapping_runs"""
        IntegrationTestBase.test_overlapping_runs(self)

    @classmethod
    def teardown_class(cls):
        cls.scheduler.shutdown()
        cls.jobstore.close()
        if os.path.exists('example.sqlite'):
            os.remove('example.sqlite')


class TestMongoDBIntegration(IntegrationTestBase):
    @staticmethod
    def make_jobstore():
        if not MongoDBJobStore:
            raise SkipTest

        return MongoDBJobStore(database='apscheduler_unittest')

    def test_overlapping_runs(self):
        """MongoDB/test_overlapping_runs"""
        IntegrationTestBase.test_overlapping_runs(self)

    @classmethod
    def teardown_class(cls):
        cls.scheduler.shutdown()
        connection = cls.jobstore.collection.database.connection
        connection.drop_database(cls.jobstore.collection.database.name)
        cls.jobstore.close()
