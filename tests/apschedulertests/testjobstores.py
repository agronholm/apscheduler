from datetime import datetime
from warnings import filterwarnings, resetwarnings
import os

from nose.tools import eq_, assert_raises

from apscheduler.jobstore.ram_store import RAMJobStore
from apscheduler.jobstore.shelve_store import ShelveJobStore
from apscheduler.jobstore.sqlalchemy_store import SQLAlchemyJobStore
from apscheduler.job import Job
from apscheduler.triggers import DateTrigger
from apscheduler.jobstore.base import JobStore


def dummy_func():
    pass


class DummyStore(JobStore):
    pass


class JobStoreTestBase(object):
    def test_add_remove_job(self):
        trigger = DateTrigger(datetime(2999, 1, 1))
        job = Job(trigger, dummy_func)
        self.jobstore.add_job(job)

        eq_(self.jobstore.get_jobs(), [job])
        eq_(job.jobstore, self.jobstore)

        self.jobstore.remove_jobs((job,))
        eq_(self.jobstore.get_jobs(), [])


class TestRamJobStore(JobStoreTestBase):
    def setup(self):
        self.jobstore = RAMJobStore()


class TestShelveJobStore(JobStoreTestBase):
    def setup(self):
        filterwarnings('ignore', category=RuntimeWarning)
        self.path = os.tempnam()
        resetwarnings()
        self.jobstore = ShelveJobStore(self.path)

    def teardown(self):
        os.remove(self.path)


class TestSQLAlchemyJobStore(JobStoreTestBase):
    def setup(self):
        self.jobstore = SQLAlchemyJobStore(url='sqlite:///')


def test_unimplemented_job_store():
    jobstore = DummyStore()
    assert_raises(NotImplementedError, getattr, jobstore, 'alias')
    assert_raises(NotImplementedError, jobstore.add_job, None)
    assert_raises(NotImplementedError, jobstore.update_jobs, ())
    assert_raises(NotImplementedError, jobstore.remove_jobs, ())
    assert_raises(NotImplementedError, jobstore.get_jobs)
