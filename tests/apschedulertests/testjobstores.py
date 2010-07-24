from datetime import datetime, timedelta
from warnings import filterwarnings, resetwarnings
import os

from nose.tools import eq_, assert_raises

from apscheduler.jobstore.ram_store import RAMJobStore
from apscheduler.jobstore.shelve_store import ShelveJobStore
from apscheduler.jobstore.sqlalchemy_store import SQLAlchemyJobStore
from apscheduler.job import SimpleJob
from apscheduler.triggers import DateTrigger
from apscheduler.jobstore.base import JobStore
from nose.plugins.skip import SkipTest


def dummy_func():
    pass


class DummyStore(JobStore):
    pass


class JobStoreTestBase(object):
    def test_add_remove_job(self):
        trigger_date = datetime(2999, 1, 1)
        trigger = DateTrigger(trigger_date)
        job = SimpleJob(trigger, dummy_func)
        job.next_run_time = trigger_date
        self.jobstore.add_job(job)

        jobs = self.jobstore.get_jobs()
        eq_(jobs, [job])
        eq_(job.jobstore, self.jobstore)

        next_run_time = self.jobstore.get_next_run_time(datetime(2998, 12, 31))
        eq_(next_run_time, trigger_date)

        trigger_date += timedelta(days=1)
        jobs[0].next_run_time = trigger_date
        self.jobstore.update_jobs(jobs)
        jobs = self.jobstore.get_jobs()
        eq_(jobs[0].next_run_time, trigger_date)

        self.jobstore.remove_jobs((job,))
        eq_(self.jobstore.get_jobs(), [])


class TestRamJobStore(JobStoreTestBase):
    def setup(self):
        self.jobstore = RAMJobStore()


class TestShelveJobStore(JobStoreTestBase):
    def setup(self):
        try:
            import dbhash
        except ImportError:
            raise SkipTest

        filterwarnings('ignore', category=RuntimeWarning)
        self.path = os.tempnam()
        resetwarnings()
        self.jobstore = ShelveJobStore(self.path)

    def teardown(self):
        os.remove(self.path)


class TestSQLAlchemyJobStore(JobStoreTestBase):
    def setup(self):
        try:
            import sqlite3
        except ImportError:
            try:
                import pysqlite2
            except ImportError:
                raise SkipTest

        self.jobstore = SQLAlchemyJobStore(url='sqlite:///')


def test_unimplemented_job_store():
    jobstore = DummyStore()
    assert_raises(NotImplementedError, getattr, jobstore, 'default_alias')
    assert_raises(NotImplementedError, jobstore.add_job, None)
    assert_raises(NotImplementedError, jobstore.update_jobs, ())
    assert_raises(NotImplementedError, jobstore.remove_jobs, ())
    assert_raises(NotImplementedError, jobstore.get_jobs)
    assert_raises(NotImplementedError, jobstore.get_next_run_time, None)
