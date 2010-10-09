from datetime import datetime
from warnings import filterwarnings, resetwarnings
import os

from nose.tools import eq_, assert_raises
from nose.plugins.skip import SkipTest

from apscheduler.jobstores.ram_store import RAMJobStore
from apscheduler.jobstores.shelve_store import ShelveJobStore
from apscheduler.jobstores.sqlalchemy_store import SQLAlchemyJobStore
from apscheduler.jobstores.base import JobStore
from apscheduler.triggers import SimpleTrigger
from apscheduler.job import Job


class DummyStore(JobStore):
    pass


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
        JobStoreTestBase.setup(self)

        try:
            filterwarnings('ignore', category=RuntimeWarning)
            from os import tempnam
            self.path = tempnam()
            resetwarnings()
            self.jobstore = ShelveJobStore(self.path)
        except ImportError:
            raise SkipTest

    def teardown(self):
        if os.path.exists(self.path):
            os.remove(self.path)

    def test_repr(self):
        eq_(repr(self.jobstore), '<ShelveJobStore (path=%s)>' % self.path)


class TestSQLAlchemyJobStore(JobStoreTestBase):
    def setup(self):
        JobStoreTestBase.setup(self)
        try:
            self.jobstore = SQLAlchemyJobStore(url='sqlite:///')
        except ImportError:
            raise SkipTest

    def test_repr(self):
        eq_(repr(self.jobstore), '<SQLAlchemyJobStore (url=sqlite:///)>')


def test_unimplemented_job_store():
    jobstore = DummyStore()
    assert_raises(NotImplementedError, getattr, jobstore, 'default_alias')
    assert_raises(NotImplementedError, jobstore.add_job, None)
    assert_raises(NotImplementedError, jobstore.update_job, None)
    assert_raises(NotImplementedError, jobstore.remove_job, None)
    assert_raises(NotImplementedError, jobstore.load_jobs)
