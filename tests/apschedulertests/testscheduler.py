from datetime import datetime

from nose.tools import eq_, raises

from apscheduler.jobstores.ram_store import RAMJobStore
from apscheduler.scheduler import Scheduler
from apscheduler.job import STATUS_OK, JobStatus, STATUS_ERROR


class TestOfflineScheduler(object):
    def setup(self):
        self.scheduler = Scheduler()

    @raises(KeyError)
    def test_jobstore_twice(self):
        self.scheduler.add_jobstore(RAMJobStore(), 'dummy')
        self.scheduler.add_jobstore(RAMJobStore(), 'dummy')

    def test_add_tentative_job(self):
        job = self.scheduler.add_date_job(lambda: None, datetime(2200, 7, 24),
                                          jobstore='dummy')
        eq_(job, None)
        eq_(self.scheduler.get_jobs(), [])

    def test_shutdown_offline(self):
        self.scheduler.shutdown()

    def test_configure_no_prefix(self):
        global_options = {'misfire_grace_time': '2',
                          'daemonic': 'false'}
        self.scheduler.configure(global_options)
        eq_(self.scheduler.misfire_grace_time, 1)
        eq_(self.scheduler.daemonic, True)
    
    def test_configure_prefix(self):
        global_options = {'apscheduler.misfire_grace_time': 2,
                          'apscheduler.daemonic': False}
        self.scheduler.configure(global_options)
        eq_(self.scheduler.misfire_grace_time, 2)
        eq_(self.scheduler.daemonic, False)

    def test_add_listener(self):
        def cb(status):
            val[0] += 1
        val = [0]
        self.scheduler.add_listener(cb, STATUS_OK)

        status = JobStatus(None, None)
        status.code = STATUS_OK
        self.scheduler._notify_listeners(status)
        eq_(val[0], 1)

        status.code = STATUS_ERROR
        self.scheduler._notify_listeners(status)
        eq_(val[0], 1)

        self.scheduler.remove_listener(cb)
        self.scheduler._notify_listeners(status)
        eq_(val[0], 1)
