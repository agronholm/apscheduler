from datetime import datetime, timedelta

from dateutil.tz import tzoffset
import pytest
import six

from apscheduler.job import Job, JobHandle
from apscheduler.triggers.date import DateTrigger
from tests.conftest import maxpython


try:
    from unittest.mock import MagicMock
except ImportError:
    from mock import MagicMock

local_tz = tzoffset('DUMMYTZ', 3600)
run_time = datetime(2010, 12, 13, 0, 8, 0, tzinfo=local_tz)
job_defaults = {'trigger': 'date', 'executor': 'default', 'trigger_args': {'run_date': run_time, 'timezone': local_tz},
                'func': '%s:dummyfunc' % __name__, 'args': (), 'kwargs': {}, 'id': 'testid', 'misfire_grace_time': 1,
                'coalesce': False, 'name': None, 'max_runs': None, 'max_instances': 1}


def dummyfunc():
    pass


def create_job(**kwargs):
    job_kwargs = job_defaults.copy()
    job_kwargs.update(kwargs)
    return Job(**job_kwargs)


@pytest.fixture
def trigger():
    return DateTrigger(local_tz, run_time)


@pytest.fixture
def job():
    return create_job()


class TestJob(object):
    def test_job_func(self):
        job = create_job(func=dummyfunc)
        assert job.func is dummyfunc

    def test_job_func_ref(self):
        job = create_job(func='%s:dummyfunc' % __name__)
        assert job.func is dummyfunc

    def test_job_bad_func(self):
        exc = pytest.raises(TypeError, create_job, func=object())
        assert 'textual reference' in str(exc.value)

    def test_job_invalid_version(self, job):
        exc = pytest.raises(ValueError, job.__setstate__, {'version': 9999})
        assert 'version' in str(exc.value)

    def test_get_run_times(self, job):
        expected_times = [run_time + timedelta(seconds=1),
                          run_time + timedelta(seconds=2)]
        job = create_job(trigger='interval', trigger_args={'seconds': 1, 'timezone': local_tz, 'start_date': run_time},
                         next_run_time=expected_times[0])
        run_times = job.get_run_times(run_time)
        assert run_times == []

        run_times = job.get_run_times(expected_times[0])
        assert run_times == [expected_times[0]]

        run_times = job.get_run_times(expected_times[1])
        assert run_times == expected_times

    def test_getstate(self, trigger):
        job = create_job(trigger=trigger, trigger_args=None)
        state = job.__getstate__()
        assert state == dict(version=1, trigger=trigger, executor='default', func='tests.test_job:dummyfunc',
                             name='dummyfunc', args=(), kwargs={}, id='testid', misfire_grace_time=1, coalesce=False,
                             max_runs=None, max_instances=1, runs=0, next_run_time=None)

    def test_setstate(self, job):
        trigger = DateTrigger(local_tz, '2010-12-14 13:05:00')
        state = dict(version=1, trigger=trigger, executor='dummyexecutor', func='tests.test_job:dummyfunc',
                     name='testjob.dummyfunc', args=[], kwargs={}, id='other_id', misfire_grace_time=2, max_runs=2,
                     coalesce=True, max_instances=2, runs=1, next_run_time=None)
        job.__setstate__(state)
        assert job.id == 'other_id'
        assert job.executor == 'dummyexecutor'
        assert job.trigger == trigger
        assert job.func == dummyfunc
        assert job.max_runs == 2
        assert job.coalesce is True
        assert job.max_instances == 2
        assert job.runs == 1
        assert job.next_run_time is None

    def test_jobs_equal(self, job):
        assert job == job

        job2 = create_job(trigger=DateTrigger(local_tz, run_time), id='otherid', func=lambda: None)
        assert not job == job2

        job2.id = job.id
        assert job == job2

        assert not job == 'bleh'

    def test_job_repr(self, job):
        assert repr(job) == '<Job (id=testid)>'


class TestJobHandle(object):
    @pytest.fixture
    def scheduler(self):
        return MagicMock()

    @pytest.fixture
    def jobhandle(self, job, scheduler):
        return JobHandle(scheduler, 'default', job)

    def test_jobhandle_modify(self, jobhandle, scheduler, job):
        new_handle = JobHandle(scheduler, 'default', job)
        new_handle._job_state['id'] = 'bar'
        new_handle._job_state['max_runs'] = 555
        scheduler.get_job = MagicMock(return_value=new_handle)

        jobhandle.modify(id='foo', max_runs=1234)
        assert scheduler.modify.called_once_with('testid', {'id': 'foo', 'max_runs': 1234})
        assert scheduler.get_job.called_once_with('foo', 'default')
        assert jobhandle.id == 'bar'
        assert jobhandle.max_runs == 555

    def test_jobhandle_remove(self, jobhandle, scheduler):
        jobhandle.remove()
        assert scheduler.remove_job.called_once_with('testid', 'default')

    def test_jobhandle_pending(self, jobhandle, scheduler, job):
        scheduler.get_jobs = MagicMock(return_value=[JobHandle(scheduler, 'default', job)])
        assert jobhandle.pending is True
        scheduler.get_jobs.assert_called_once_with('default', True)

        scheduler.get_jobs = MagicMock(return_value=[])
        assert jobhandle.pending is False
        scheduler.get_jobs.assert_called_once_with('default', True)

    def test_jobhandle_getattr_fail(self, jobhandle):
        exc = pytest.raises(AttributeError, getattr, jobhandle, 'foo')
        assert 'foo' in str(exc.value)

    def test_jobhandle_setattr(self, jobhandle, scheduler, job):
        new_handle = JobHandle(scheduler, 'default', job)
        new_handle._job_state['max_runs'] = 555
        scheduler.get_job = MagicMock(return_value=new_handle)

        jobhandle.max_runs = 1234
        assert scheduler.modify.called_once_with('testid', {'id': 'foo', 'max_runs': 1234})
        assert scheduler.get_job.called_once_with('foo', 'default')
        assert jobhandle.max_runs == 555

    def test_jobhandle_equals(self, jobhandle, job):
        assert jobhandle == JobHandle(None, 'foo', job)
        assert not jobhandle == 'bah'

    def test_jobhandle_repr(self, jobhandle):
        assert repr(jobhandle) == '<JobHandle (id=testid name=dummyfunc)>'

    def test_jobhandle_str(self, jobhandle):
        assert str(jobhandle) == 'dummyfunc (trigger: date[2010-12-13 00:08:00 DUMMYTZ], next run at: None)'

    @maxpython(3, 0)
    def test_jobhandle_unicode(self, jobhandle):
        assert jobhandle.__unicode__() == six.u(
            'dummyfunc (trigger: date[2010-12-13 00:08:00 DUMMYTZ], next run at: None)')
