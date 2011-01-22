from datetime import datetime
from warnings import filterwarnings, resetwarnings
from tempfile import NamedTemporaryFile
import os

from nose.tools import eq_, assert_raises, raises
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


def dummy_job():
    pass


class JobStoreTestBase(object):
    def setup(self):
        self.trigger_date = datetime(2999, 1, 1)
        self.earlier_date = datetime(2998, 12, 31)
        self.trigger = SimpleTrigger(self.trigger_date)
        self.job = Job(self.trigger, dummy_job, [], {})
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


class TestRAMJobStore(JobStoreTestBase):
    def setup(self):
        JobStoreTestBase.setup(self)
        self.jobstore = RAMJobStore()

    @SkipTest
    def test_jobstore_add_update_remove(self):
        pass

    def test_repr(self):
        eq_(repr(self.jobstore), '<RAMJobStore>')


class TestShelveJobStore(JobStoreTestBase):
    def setup(self):
        if not ShelveJobStore:
            raise SkipTest

        JobStoreTestBase.setup(self)
        filterwarnings('ignore', category=RuntimeWarning)
        f = NamedTemporaryFile(prefix='apscheduler_')
        f.close()
        self.path = f.name
        resetwarnings()
        self.jobstore = ShelveJobStore(self.path)

    def teardown(self):
        if os.path.exists(self.path):
            os.remove(self.path)

    def test_repr(self):
        eq_(repr(self.jobstore), '<ShelveJobStore (path=%s)>' % self.path)


class TestSQLAlchemyJobStore1(JobStoreTestBase):
    def setup(self):
        if not SQLAlchemyJobStore:
            raise SkipTest

        JobStoreTestBase.setup(self)
        self.jobstore = SQLAlchemyJobStore(url='sqlite:///')

    def test_repr(self):
        eq_(repr(self.jobstore), '<SQLAlchemyJobStore (url=sqlite:///)>')


class TestSQLAlchemyJobStore2(JobStoreTestBase):
    def setup(self):
        if not SQLAlchemyJobStore:
            raise SkipTest

        from sqlalchemy import create_engine

        JobStoreTestBase.setup(self)
        engine = create_engine('sqlite:///')
        self.jobstore = SQLAlchemyJobStore(engine=engine)

    def test_repr(self):
        eq_(repr(self.jobstore), '<SQLAlchemyJobStore (url=sqlite:///)>')


class TestMongoDBJobStore(JobStoreTestBase):
    def setup(self):
        if not MongoDBJobStore:
            raise SkipTest

        JobStoreTestBase.setup(self)
        self.jobstore = MongoDBJobStore()

    def teardown(self):
        connection = self.jobstore.collection.database.connection
        connection.drop_database(self.jobstore._dbname)

    def test_repr(self):
        eq_(repr(self.jobstore),
            "<MongoDBJobStore (connection=Connection('localhost', 27017))>")


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
    class DummyJobStore(JobStore): pass
    jobstore = DummyJobStore()
    assert_raises(NotImplementedError, jobstore.add_job, None)
    assert_raises(NotImplementedError, jobstore.update_job, None)
    assert_raises(NotImplementedError, jobstore.remove_job, None)
    assert_raises(NotImplementedError, jobstore.load_jobs)
