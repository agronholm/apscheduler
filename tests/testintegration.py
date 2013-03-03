from time import sleep
from warnings import filterwarnings, resetwarnings
from tempfile import NamedTemporaryFile
import os

from nose.tools import eq_
from nose.plugins.skip import SkipTest

from apscheduler.scheduler import Scheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_MISSED

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

try:
    from apscheduler.jobstores.redis_store import RedisJobStore
except ImportError:
    RedisJobStore = None


def increment(vals, sleeptime):
    vals[0] += 1
    sleep(sleeptime)


class IntegrationTestBase(object):
    def setup(self):
        self.jobstore = self.make_jobstore()
        self.scheduler = Scheduler()
        self.scheduler.add_jobstore(self.jobstore, 'persistent')
        self.scheduler.start()

    def test_overlapping_runs(self):
        # Makes sure that "increment" is only ran once, since it will still be
        # running when the next appointed time hits.

        vals = [0]
        self.scheduler.add_interval_job(increment, jobstore='persistent', seconds=1, args=[vals, 2])
        sleep(2.5)
        eq_(vals, [1])

    def test_max_instances(self):
        vals = [0]
        events = []
        self.scheduler.add_listener(events.append, EVENT_JOB_EXECUTED | EVENT_JOB_MISSED)
        self.scheduler.add_interval_job(increment, jobstore='persistent', seconds=0.3, max_instances=2, max_runs=4,
                                        args=[vals, 1])
        sleep(2.4)
        eq_(vals, [2])
        eq_(len(events), 4)
        eq_(events[0].code, EVENT_JOB_MISSED)
        eq_(events[1].code, EVENT_JOB_MISSED)
        eq_(events[2].code, EVENT_JOB_EXECUTED)
        eq_(events[3].code, EVENT_JOB_EXECUTED)


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

    def test_max_instances(self):
        """Shelve/test_max_instances"""
        IntegrationTestBase.test_max_instances(self)

    def teardown(self):
        self.scheduler.shutdown()
        self.jobstore.close()
        if os.path.exists(self.jobstore.path):
            os.remove(self.jobstore.path)


class TestSQLAlchemyIntegration(IntegrationTestBase):
    @staticmethod
    def make_jobstore():
        if not SQLAlchemyJobStore:
            raise SkipTest

        return SQLAlchemyJobStore(url='sqlite:///example.sqlite')

    def test_overlapping_runs(self):
        """SQLAlchemy/test_overlapping_runs"""
        IntegrationTestBase.test_overlapping_runs(self)

    def test_max_instances(self):
        """SQLAlchemy/test_max_instances"""
        IntegrationTestBase.test_max_instances(self)

    def teardown(self):
        self.scheduler.shutdown()
        self.jobstore.close()
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

    def test_max_instances(self):
        """MongoDB/test_max_instances"""
        IntegrationTestBase.test_max_instances(self)

    def teardown(self):
        self.scheduler.shutdown()
        connection = self.jobstore.collection.database.connection
        connection.drop_database(self.jobstore.collection.database.name)
        self.jobstore.close()


class TestRedisIntegration(IntegrationTestBase):
    @staticmethod
    def make_jobstore():
        if not RedisJobStore:
            raise SkipTest

        return RedisJobStore(db='apscheduler_unittest')

    def test_overlapping_runs(self):
        """Redis/test_overlapping_runs"""
        IntegrationTestBase.test_overlapping_runs(self)

    def test_max_instances(self):
        """Redis/test_max_instances"""
        IntegrationTestBase.test_max_instances(self)

    def teardown(self):
        self.scheduler.shutdown()
        self.jobstore.redis.flushdb()
        self.jobstore.close()
