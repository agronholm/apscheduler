from datetime import datetime, timedelta
from threading import Lock

from nose.tools import eq_, assert_raises  # @UnresolvedImport

from apscheduler.job import Job, MaxInstancesReachedError
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger


lock_type = type(Lock())


def dummyfunc():
    pass


class TestJob(object):
    RUNTIME = datetime(2010, 12, 13, 0, 8, 0)

    def setup(self):
        self.trigger = DateTrigger(self.RUNTIME)
        self.job = Job(self.trigger, dummyfunc, [], {}, 1, False, None, None, 1)

    def test_compute_next_run_time(self):
        self.job.compute_next_run_time(self.RUNTIME - timedelta(microseconds=1))
        eq_(self.job.next_run_time, self.RUNTIME)

        self.job.compute_next_run_time(self.RUNTIME)
        eq_(self.job.next_run_time, self.RUNTIME)

        self.job.compute_next_run_time(self.RUNTIME + timedelta(microseconds=1))
        eq_(self.job.next_run_time, None)

    def test_compute_run_times(self):
        expected_times = [self.RUNTIME + timedelta(seconds=1),
                          self.RUNTIME + timedelta(seconds=2)]
        self.job.trigger = IntervalTrigger(seconds=1, start_date=self.RUNTIME)
        self.job.compute_next_run_time(expected_times[0])
        eq_(self.job.next_run_time, expected_times[0])

        run_times = self.job.get_run_times(self.RUNTIME)
        eq_(run_times, [])

        run_times = self.job.get_run_times(expected_times[0])
        eq_(run_times, [expected_times[0]])

        run_times = self.job.get_run_times(expected_times[1])
        eq_(run_times, expected_times)

    def test_max_runs(self):
        self.job.max_runs = 1
        self.job.runs += 1
        self.job.compute_next_run_time(self.RUNTIME)
        eq_(self.job.next_run_time, None)

    def test_eq_num(self):
        # Just increasing coverage here
        assert not self.job == 'dummyfunc'

    def test_getstate(self):
        state = self.job.__getstate__()
        eq_(state, dict(trigger=self.trigger,
                        func_ref='testjob:dummyfunc',
                        name='dummyfunc', args=[],
                        kwargs={}, misfire_grace_time=1,
                        coalesce=False, max_runs=None,
                        max_instances=1, runs=0))

    def test_setstate(self):
        trigger = DateTrigger('2010-12-14 13:05:00')
        state = dict(trigger=trigger, name='testjob.dummyfunc',
                     func_ref='testjob:dummyfunc',
                     args=[], kwargs={}, misfire_grace_time=2, max_runs=2,
                     coalesce=True, max_instances=2, runs=1)
        self.job.__setstate__(state)
        eq_(self.job.trigger, trigger)
        eq_(self.job.func, dummyfunc)
        eq_(self.job.max_runs, 2)
        eq_(self.job.coalesce, True)
        eq_(self.job.max_instances, 2)
        eq_(self.job.runs, 1)
        assert not hasattr(self.job, 'func_ref')
        assert isinstance(self.job._lock, lock_type)

    def test_jobs_equal(self):
        assert self.job == self.job

        job2 = Job(DateTrigger(self.RUNTIME), lambda: None, [], {}, 1, False, None, None, 1)
        assert self.job != job2

        job2.id = self.job.id = 123
        eq_(self.job, job2)

        assert self.job != 'bleh'

    def test_instances(self):
        self.job.max_instances = 2
        eq_(self.job.instances, 0)

        self.job.add_instance()
        eq_(self.job.instances, 1)

        self.job.add_instance()
        eq_(self.job.instances, 2)

        assert_raises(MaxInstancesReachedError, self.job.add_instance)

        self.job.remove_instance()
        eq_(self.job.instances, 1)

        self.job.remove_instance()
        eq_(self.job.instances, 0)

        assert_raises(AssertionError, self.job.remove_instance)

    def test_repr(self):
        self.job.compute_next_run_time(self.RUNTIME)
        eq_(repr(self.job),
            "<Job (name=dummyfunc, trigger=<DateTrigger (run_date=datetime.datetime(2010, 12, 13, 0, 8))>)>")
        eq_(str(self.job),
            "dummyfunc (trigger: date[2010-12-13 00:08:00], next run at: 2010-12-13 00:08:00)")
