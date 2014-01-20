from datetime import datetime, timedelta
from threading import Lock

from dateutil.tz import tzoffset
import pytest

from apscheduler.job import Job, MaxInstancesReachedError
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger


lock_type = type(Lock())
local_tz = tzoffset('DUMMYTZ', 3600)
defaults = {'timezone': local_tz}


def dummyfunc():
    pass


class TestJob(object):
    RUNTIME = datetime(2010, 12, 13, 0, 8, 0, tzinfo=local_tz)

    @pytest.fixture
    def trigger(self):
        return DateTrigger(defaults, self.RUNTIME)

    @pytest.fixture
    def job(self, trigger):
        return Job(trigger, dummyfunc, [], {}, 1, False, None, None, 1)

    def test_compute_next_run_time(self, job):
        job.compute_next_run_time(self.RUNTIME - timedelta(microseconds=1))
        assert job.next_run_time == self.RUNTIME

        job.compute_next_run_time(self.RUNTIME)
        assert job.next_run_time == self.RUNTIME

        job.compute_next_run_time(self.RUNTIME + timedelta(microseconds=1))
        assert job.next_run_time is None

    def test_compute_run_times(self, job):
        expected_times = [self.RUNTIME + timedelta(seconds=1),
                          self.RUNTIME + timedelta(seconds=2)]
        job.trigger = IntervalTrigger(defaults, seconds=1, start_date=self.RUNTIME)
        job.compute_next_run_time(expected_times[0])
        assert job.next_run_time == expected_times[0]

        run_times = job.get_run_times(self.RUNTIME)
        assert run_times == []

        run_times = job.get_run_times(expected_times[0])
        assert run_times == [expected_times[0]]

        run_times = job.get_run_times(expected_times[1])
        assert run_times == expected_times

    def test_max_runs(self, job):
        job.max_runs = 1
        job.runs += 1
        job.compute_next_run_time(self.RUNTIME)
        assert job.next_run_time is None

    def test_eq_num(self, job):
        # Just increasing coverage here
        assert not job == 'dummyfunc'

    def test_getstate(self, job, trigger):
        state = job.__getstate__()
        assert state == dict(trigger=trigger,
                             func_ref='tests.test_job:dummyfunc',
                             name='dummyfunc', args=[],
                             kwargs={}, misfire_grace_time=1,
                             coalesce=False, max_runs=None,
                             max_instances=1, runs=0)

    def test_setstate(self, job):
        trigger = DateTrigger(defaults, '2010-12-14 13:05:00')
        state = dict(trigger=trigger, name='testjob.dummyfunc',
                     func_ref='tests.test_job:dummyfunc',
                     args=[], kwargs={}, misfire_grace_time=2, max_runs=2,
                     coalesce=True, max_instances=2, runs=1)
        job.__setstate__(state)
        assert job.trigger == trigger
        assert job.func == dummyfunc
        assert job.max_runs == 2
        assert job.coalesce is True
        assert job.max_instances == 2
        assert job.runs == 1
        assert not hasattr(job, 'func_ref')
        assert isinstance(job._lock, lock_type)

    def test_jobs_equal(self, job):
        assert job == job

        job2 = Job(DateTrigger(defaults, self.RUNTIME), lambda: None, [], {}, 1, False, None, None, 1)
        assert job != job2

        job2.id = job.id = 123
        assert job == job2

        assert job != 'bleh'

    def test_instances(self, job):
        job.max_instances = 2
        assert job.instances == 0

        job.add_instance()
        assert job.instances == 1

        job.add_instance()
        assert job.instances == 2

        with pytest.raises(MaxInstancesReachedError):
            job.add_instance()

        job.remove_instance()
        assert job.instances == 1

        job.remove_instance()
        assert job.instances == 0

        with pytest.raises(AssertionError):
            job.remove_instance()

    def test_repr(self, job):
        job.compute_next_run_time(self.RUNTIME)
        assert repr(job) == \
            "<Job (name=dummyfunc, trigger=<DateTrigger (run_date='2010-12-13 00:08:00 DUMMYTZ')>)>"
        assert str(job) == \
            "dummyfunc (trigger: date[2010-12-13 00:08:00 DUMMYTZ], next run at: 2010-12-13 00:08:00 DUMMYTZ)"
