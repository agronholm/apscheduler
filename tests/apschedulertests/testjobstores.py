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
from apscheduler.job import JobMeta


class StatefulJob(object):
    counter = 0

    def run(self):
        self.counter += 1


class JobStoreTestBase(object):
    def setup(self):
        self.trigger_date = datetime(2999, 1, 1)
        self.earlier_date = datetime(2998, 12, 31)
        self.trigger = SimpleTrigger(self.trigger_date)
        self.job = StatefulJob()
        self.jobmeta = JobMeta(self.job, self.trigger)
        self.jobmeta.next_run_time = self.trigger_date

    def test_jobstore_add_list_remove(self):
        self.jobstore.add_job(self.jobmeta)

        jobmetas = self.jobstore.list_jobs()
        eq_(jobmetas, [self.jobmeta])
        eq_(jobmetas[0].jobstore, self.jobstore)

        self.jobstore.remove_job(jobmetas[0])
        eq_(self.jobstore.list_jobs(), [])

    def test_jobstore_add_checkout_update(self):
        jobmetas = self.jobstore.checkout_jobs(self.trigger_date)
        eq_(jobmetas, [])

        self.jobstore.add_job(self.jobmeta)
        eq_(self.jobmeta.checkin_time, None)

        jobmetas = self.jobstore.checkout_jobs(self.earlier_date)
        eq_(jobmetas, [])

        jobmetas = self.jobstore.checkout_jobs(self.trigger_date)
        eq_(jobmetas, [self.jobmeta])
        eq_(jobmetas[0].jobstore, self.jobstore)
        eq_(jobmetas[0].checkin_time, None)
        eq_(jobmetas[0].job.counter, 0)

        jobmetas[0].job.run()
        self.jobstore.checkin_job(jobmetas[0])

        jobmetas = self.jobstore.list_jobs()
        eq_(jobmetas, [self.jobmeta])
        eq_(jobmetas[0].job.counter, 1)


class TestRAMJobStore(JobStoreTestBase):
    def setup(self):
        JobStoreTestBase.setup(self)
        self.jobstore = RAMJobStore()

    def test_repr(self):
        eq_(repr(self.jobstore), 'RAMJobStore')


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
        eq_(repr(self.jobstore), 'ShelveJobStore (%s)' % self.path)


class TestSQLAlchemyJobStore(JobStoreTestBase):
    def setup(self):
        JobStoreTestBase.setup(self)
        try:
            self.jobstore = SQLAlchemyJobStore(url='sqlite:///')
        except ImportError:
            raise SkipTest

    def test_repr(self):
        eq_(repr(self.jobstore), 'SQLAlchemyJobStore (sqlite:///)')


def test_unimplemented_job_store():
    class DummyStore(JobStore): pass
    jobstore = DummyStore()
    assert_raises(NotImplementedError, getattr, jobstore, 'default_alias')
    assert_raises(NotImplementedError, jobstore.add_job, None)
    assert_raises(NotImplementedError, jobstore.checkin_job, None)
    assert_raises(NotImplementedError, jobstore.remove_job, None)
    assert_raises(NotImplementedError, jobstore.checkout_jobs, None)
    assert_raises(NotImplementedError, jobstore.list_jobs)
    assert_raises(NotImplementedError, jobstore.get_next_run_time, None)
