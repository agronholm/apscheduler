from datetime import datetime
from warnings import filterwarnings, resetwarnings
from tempfile import NamedTemporaryFile
import os

from nose.tools import eq_, assert_raises, raises  # @UnresolvedImport
from nose.plugins.skip import SkipTest

from apscheduler.jobstores.ram_store import RAMJobStore
from apscheduler.jobstores.base import JobStore
from apscheduler.triggers import SimpleTrigger
from apscheduler.job import Job

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


def dummy_job():
    pass


def dummy_job2():
    pass


def dummy_job3():
    pass


class JobStoreTestBase(object):
    def setup(self):
        self.trigger_date = datetime(2999, 1, 1)
        self.earlier_date = datetime(2998, 12, 31)
        self.trigger = SimpleTrigger(self.trigger_date)
        self.job = Job(self.trigger, dummy_job, [], {}, 1, False)
        self.job.next_run_time = self.trigger_date

    def test_jobstore_add_update_remove(self):
        eq_(self.jobstore.jobs, [])

        self.jobstore.add_job(self.job)
        eq_(self.jobstore.jobs, [self.job])
        eq_(self.jobstore.jobs[0], self.job)
        eq_(self.jobstore.jobs[0].runs, 0)

        self.job.runs += 1
        self.jobstore.update_job(self.job)
        self.jobstore.load_jobs()
        eq_(len(self.jobstore.jobs), 1)
        eq_(self.jobstore.jobs, [self.job])
        eq_(self.jobstore.jobs[0].runs, 1)

        self.jobstore.remove_job(self.job)
        eq_(self.jobstore.jobs, [])
        self.jobstore.load_jobs()
        eq_(self.jobstore.jobs, [])


class PersistentJobstoreTestBase(JobStoreTestBase):
    def test_one_job_fails_to_load(self):
        global dummy_job2, dummy_job_temp
        job1 = Job(self.trigger, dummy_job, [], {}, 1, False)
        job2 = Job(self.trigger, dummy_job2, [], {}, 1, False)
        job3 = Job(self.trigger, dummy_job3, [], {}, 1, False)
        for job in job1, job2, job3:
            job.next_run_time = self.trigger_date
            self.jobstore.add_job(job)

        dummy_job_temp = dummy_job2
        del dummy_job2
        try:
            self.jobstore.load_jobs()
            eq_(len(self.jobstore.jobs), 2)
        finally:
            dummy_job2 = dummy_job_temp
            del dummy_job_temp


class TestRAMJobStore(JobStoreTestBase):
    @classmethod
    def setup_class(cls):
        cls.jobstore = RAMJobStore()

    def test_repr(self):
        eq_(repr(self.jobstore), '<RAMJobStore>')


class TestShelveJobStore(PersistentJobstoreTestBase):
    @classmethod
    def setup_class(cls):
        if not ShelveJobStore:
            raise SkipTest

        filterwarnings('ignore', category=RuntimeWarning)
        f = NamedTemporaryFile(prefix='apscheduler_')
        f.close()
        resetwarnings()
        cls.jobstore = ShelveJobStore(f.name)

    @classmethod
    def teardown_class(cls):
        cls.jobstore.close()
        if os.path.exists(cls.jobstore.path):
            os.remove(cls.jobstore.path)

    def test_repr(self):
        eq_(repr(self.jobstore),
            '<ShelveJobStore (path=%s)>' % self.jobstore.path)


class TestSQLAlchemyJobStore1(PersistentJobstoreTestBase):
    @classmethod
    def setup_class(cls):
        if not SQLAlchemyJobStore:
            raise SkipTest

        cls.jobstore = SQLAlchemyJobStore(url='sqlite:///')

    @classmethod
    def teardown_class(cls):
        cls.jobstore.close()

    def test_repr(self):
        eq_(repr(self.jobstore), '<SQLAlchemyJobStore (url=sqlite:///)>')


class TestSQLAlchemyJobStore2(PersistentJobstoreTestBase):
    @classmethod
    def setup_class(cls):
        if not SQLAlchemyJobStore:
            raise SkipTest

        from sqlalchemy import create_engine

        engine = create_engine('sqlite:///')
        cls.jobstore = SQLAlchemyJobStore(engine=engine)

    @classmethod
    def teardown_class(cls):
        cls.jobstore.close()

    def test_repr(self):
        eq_(repr(self.jobstore), '<SQLAlchemyJobStore (url=sqlite:///)>')


class TestMongoDBJobStore(PersistentJobstoreTestBase):
    @classmethod
    def setup_class(cls):
        if not MongoDBJobStore:
            raise SkipTest

        cls.jobstore = MongoDBJobStore(database='apscheduler_unittest')

    @classmethod
    def teardown_class(cls):
        connection = cls.jobstore.collection.database.connection
        connection.drop_database(cls.jobstore.collection.database.name)
        cls.jobstore.close()

    def test_repr(self):
        eq_(repr(self.jobstore), "<MongoDBJobStore (connection=Connection('localhost', 27017))>")


class TestRedisJobStore(PersistentJobstoreTestBase):
    @classmethod
    def setup_class(cls):
        if not RedisJobStore:
            raise SkipTest

        cls.jobstore = RedisJobStore(db='apscheduler_unittest')

    @classmethod
    def teardown_class(cls):
        cls.jobstore.redis.flushdb()
        cls.jobstore.close()

    def test_repr(self):
        eq_(repr(self.jobstore), "<RedisJobStore>")


@raises(ValueError)
def test_sqlalchemy_invalid_args():
    if not SQLAlchemyJobStore:
        raise SkipTest

    SQLAlchemyJobStore()


def test_sqlalchemy_alternate_tablename():
    if not SQLAlchemyJobStore:
        raise SkipTest

    store = SQLAlchemyJobStore('sqlite:///', tablename='test_table')
    eq_(store.jobs_t.name, 'test_table')


def test_unimplemented_job_store():
    class DummyJobStore(JobStore):
        pass

    jobstore = DummyJobStore()
    assert_raises(NotImplementedError, jobstore.add_job, None)
    assert_raises(NotImplementedError, jobstore.update_job, None)
    assert_raises(NotImplementedError, jobstore.remove_job, None)
    assert_raises(NotImplementedError, jobstore.load_jobs)
