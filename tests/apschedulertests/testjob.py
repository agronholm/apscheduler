from datetime import datetime, timedelta
from threading import Lock

from nose.tools import eq_, raises, assert_raises

from apscheduler.job import Job
from apscheduler.triggers.simple import SimpleTrigger


lock_type = type(Lock())

def dummyfunc():
    pass


class TestJob(object):
    RUNTIME = datetime(2010, 12, 13, 0, 8, 0)

    def setup(self):
        self.trigger = SimpleTrigger(self.RUNTIME)
        self.job = Job(self.trigger, dummyfunc, [], {})

    def test_compute_next_run_time(self):
        self.job.compute_next_run_time(self.RUNTIME - timedelta(microseconds=1))
        eq_(self.job.next_run_time, self.RUNTIME)

        self.job.compute_next_run_time(self.RUNTIME)
        eq_(self.job.next_run_time, self.RUNTIME)

        self.job.compute_next_run_time(self.RUNTIME + timedelta(microseconds=1))
        eq_(self.job.next_run_time, None)

    def test_max_runs(self):
        self.job.max_runs = 1
        self.job.runs += 1
        self.job.compute_next_run_time(self.RUNTIME)
        eq_(self.job.next_run_time, None)         

    def test_getstate(self):
        state = self.job.__getstate__()
        eq_(state, dict(trigger=self.trigger,
                        name='apschedulertests.testjob.dummyfunc', args=[],
                        kwargs={}, misfire_grace_time=1, max_runs=None,
                        max_concurrency=1, runs=0))

    def test_setstate(self):
        trigger = SimpleTrigger('2010-12-14 13:05:00')
        state = dict(trigger=trigger, name='apschedulertests.testjob.dummyfunc',
                     func_ref='apschedulertests.testjob:dummyfunc',
                     args=[], kwargs={}, misfire_grace_time=2, max_runs=2,
                     max_concurrency=2, runs=1)
        self.job.__setstate__(state)
        eq_(self.job.trigger, trigger)
        eq_(self.job.func, dummyfunc)
        eq_(self.job.max_runs, 2)
        eq_(self.job.max_concurrency, 2)
        eq_(self.job.runs, 1)
        assert not hasattr(self.job, 'func_ref')
        assert isinstance(self.job._lock, lock_type)

    def test_jobs_equal(self):
        assert self.job == self.job

        job2 = Job(SimpleTrigger(self.RUNTIME), lambda: None, [], {})
        assert self.job != job2

        job2.id = self.job.id = 123
        eq_(self.job, job2)
        
        assert self.job != 'bleh'

    def test_instances(self):
        eq_(self.job.instances, 0)
        self.job.add_instance()
        eq_(self.job.instances, 1)
        self.job.add_instance()
        eq_(self.job.instances, 2)
        self.job.remove_instance()
        eq_(self.job.instances, 1)
        self.job.remove_instance()
        eq_(self.job.instances, 0)
        assert_raises(ValueError, self.job.remove_instance)

    def test_repr(self):
        self.job.compute_next_run_time(self.RUNTIME)
        eq_(repr(self.job),
            "<Job (name=apschedulertests.testjob.dummyfunc, "
            "trigger=<SimpleTrigger (run_date=datetime.datetime(2010, 12, 13, 0, 8))>)>")
        eq_(str(self.job),
            "apschedulertests.testjob.dummyfunc "
            "(trigger: date[2010-12-13 00:08:00], "
            "next run at: 2010-12-13 00:08:00)")


@raises(ValueError)
def test_create_job_no_trigger():
    Job(None, lambda: None, [], {})


@raises(TypeError)
def test_create_job_invalid_func():
    Job(SimpleTrigger(datetime.now()), 'bleh', [], {})


@raises(TypeError)
def test_create_job_invalid_args():
    Job(SimpleTrigger(datetime.now()), lambda: None, None, {})


@raises(TypeError)
def test_create_job_invalid_kwargs():
    Job(SimpleTrigger(datetime.now()), lambda: None, [], None)


@raises(ValueError)
def test_create_job_invalid_misfire():
    Job(SimpleTrigger(datetime.now()), lambda: None, [], {},
        misfire_grace_time=0)


@raises(ValueError)
def test_create_job_invalid_maxruns():
    Job(SimpleTrigger(datetime.now()), lambda: None, [], {}, max_runs=0)


@raises(ValueError)
def test_create_job_invalid_maxconcurrency():
    Job(SimpleTrigger(datetime.now()), lambda: None, [], {}, max_concurrency=0)
