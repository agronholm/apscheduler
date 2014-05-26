# coding: utf-8
from datetime import datetime, timedelta
from functools import partial

import pytest
import six

from apscheduler.triggers.date import DateTrigger
from tests.conftest import maxpython

try:
    from unittest.mock import MagicMock
except ImportError:
    from mock import MagicMock


def dummyfunc():
    pass


@pytest.fixture
def job(create_job):
    return create_job(func=dummyfunc)


class TestJob(object):
    def test_job_func(self, create_job):
        """Tests that Job can accept a plain, direct function."""

        job = create_job(func=dummyfunc)
        assert job.func is dummyfunc

    def test_job_non_inspectable_func(self, create_job):
        """Tests that Job can accept a function that fails on inspect.getargspec()."""

        func = partial(dummyfunc)
        job = create_job(func=func)
        assert job.func is func

    def test_job_func_ref(self, create_job):
        """Tests that Job can accept a function by its textual reference."""

        job = create_job(func='%s:dummyfunc' % __name__)
        assert job.func is dummyfunc

    def test_job_bad_func(self, create_job):
        exc = pytest.raises(TypeError, create_job, func=object())
        assert 'textual reference' in str(exc.value)

    def test_job_invalid_version(self, job):
        exc = pytest.raises(ValueError, job.__setstate__, {'version': 9999})
        assert 'version' in str(exc.value)

    def test_remove(self, job):
        assert job._scheduler.remove_job.called_once_with('testid', 'default')

    def test_get_run_times(self, create_job, timezone):
        run_time = timezone.localize(datetime(2010, 12, 13, 0, 8))
        expected_times = [run_time + timedelta(seconds=1),
                          run_time + timedelta(seconds=2)]
        job = create_job(trigger='interval', trigger_args={'seconds': 1, 'timezone': timezone, 'start_date': run_time},
                         next_run_time=expected_times[0], func=dummyfunc)
        run_times = job.get_run_times(run_time)
        assert run_times == []

        run_times = job.get_run_times(expected_times[0])
        assert run_times == [expected_times[0]]

        run_times = job.get_run_times(expected_times[1])
        assert run_times == expected_times

    def test_pending(self, job):
        """Tests that the "pending" property return True when _jobstore is a string, False otherwise."""

        assert not job.pending

        job._jobstore = 'test'
        assert job.pending

    def test_getstate(self, job):
        state = job.__getstate__()
        expected = dict(version=1, trigger=job.trigger, executor='default', func='tests.test_job:dummyfunc',
                        name=b'n\xc3\xa4m\xc3\xa9'.decode('utf-8'), args=(), kwargs={},
                        id=b't\xc3\xa9st\xc3\xafd'.decode('utf-8'), misfire_grace_time=1, coalesce=False,
                        max_runs=None, max_instances=1, runs=0, next_run_time=None)
        assert state == expected

    def test_setstate(self, job, timezone):
        trigger = DateTrigger('2010-12-14 13:05:00', timezone)
        state = dict(version=1, scheduler=MagicMock(), jobstore=MagicMock(), trigger=trigger, executor='dummyexecutor',
                     func='tests.test_job:dummyfunc', name='testjob.dummyfunc', args=[], kwargs={}, id='other_id',
                     misfire_grace_time=2, max_runs=2, coalesce=True, max_instances=2, runs=1, next_run_time=None)
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

    def test_eq(self, create_job, timezone):
        job = create_job(func=dummyfunc)
        assert job == job

        job2 = create_job(trigger='date', id='otherid', trigger_args={'run_date': datetime.now(), 'timezone': timezone},
                          func=dummyfunc)
        assert not job == job2

        job2.id = job.id
        assert job == job2

        assert not job == 'bleh'

    def test_repr(self, job):
        if six.PY2:
            assert repr(job) == '<Job (id=t\\xe9st\\xefd name=n\\xe4m\\xe9)>'
        else:
            assert repr(job) == b'<Job (id=t\xc3\xa9st\xc3\xafd name=n\xc3\xa4m\xc3\xa9)>'.decode('utf-8')

    def test_str(self, job):
        if six.PY2:
            expected = 'n\\xe4m\\xe9 (trigger: date[2011-04-03 18:40:00 CEST], next run at: None)'
        else:
            expected = b'n\xc3\xa4m\xc3\xa9 (trigger: date[2011-04-03 18:40:00 CEST], next run at: None)'.\
                decode('utf-8')

        assert str(job) == expected

    @maxpython(3, 0)
    def test_unicode(self, job):
        assert job.__unicode__() == \
            b'n\xc3\xa4m\xc3\xa9 (trigger: date[2011-04-03 18:40:00 CEST], next run at: None)'.decode('utf-8')
