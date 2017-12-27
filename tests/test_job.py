# coding: utf-8
from datetime import datetime, timedelta
from functools import partial

import pytest
import six

from apscheduler.job import Job
from apscheduler.schedulers.base import BaseScheduler
from apscheduler.triggers.date import DateTrigger

try:
    from unittest.mock import MagicMock, patch
except ImportError:
    from mock import MagicMock, patch


def dummyfunc():
    pass


@pytest.fixture
def job(create_job):
    return create_job(func=dummyfunc)


@pytest.mark.parametrize('job_id', ['testid', None])
def test_constructor(job_id):
    with patch('apscheduler.job.Job._modify') as _modify:
        scheduler_mock = MagicMock(BaseScheduler)
        job = Job(scheduler_mock, id=job_id)
        assert job._scheduler is scheduler_mock
        assert job._jobstore_alias is None

        modify_kwargs = _modify.call_args[1]
        if job_id is None:
            assert len(modify_kwargs['id']) == 32
        else:
            assert modify_kwargs['id'] == job_id


def test_modify(job):
    job.modify(bah=1, foo='x')
    job._scheduler.modify_job.assert_called_once_with(job.id, None, bah=1, foo='x')


def test_reschedule(job):
    job.reschedule('trigger', bah=1, foo='x')
    job._scheduler.reschedule_job.assert_called_once_with(job.id, None, 'trigger', bah=1, foo='x')


def test_pause(job):
    job.pause()
    job._scheduler.pause_job.assert_called_once_with(job.id, None)


def test_resume(job):
    job.resume()
    job._scheduler.resume_job.assert_called_once_with(job.id, None)


def test_remove(job):
    job.remove()
    job._scheduler.remove_job.assert_called_once_with(job.id, None)


def test_pending(job):
    """
    Tests that the "pending" property return True when _jobstore_alias is a string, ``False``
    otherwise.

    """
    assert job.pending

    job._jobstore_alias = 'test'
    assert not job.pending


def test_get_run_times(create_job, timezone):
    run_time = timezone.localize(datetime(2010, 12, 13, 0, 8))
    expected_times = [run_time + timedelta(seconds=1), run_time + timedelta(seconds=2)]
    job = create_job(trigger='interval',
                     trigger_args={'seconds': 1, 'timezone': timezone, 'start_date': run_time},
                     next_run_time=expected_times[0], func=dummyfunc)

    run_times = job._get_run_times(run_time)
    assert run_times == []

    run_times = job._get_run_times(expected_times[0])
    assert run_times == [expected_times[0]]

    run_times = job._get_run_times(expected_times[1])
    assert run_times == expected_times


def test_private_modify_bad_id(job):
    """Tests that only strings are accepted for job IDs."""
    del job.id
    exc = pytest.raises(TypeError, job._modify, id=3)
    assert str(exc.value) == 'id must be a nonempty string'


def test_private_modify_id(job):
    """Tests that the job ID can't be changed."""
    exc = pytest.raises(ValueError, job._modify, id='alternate')
    assert str(exc.value) == 'The job ID may not be changed'


def test_private_modify_bad_func(job):
    """Tests that given a func of something else than a callable or string raises a TypeError."""
    exc = pytest.raises(TypeError, job._modify, func=1)
    assert str(exc.value) == 'func must be a callable or a textual reference to one'


def test_private_modify_func_ref(job):
    """Tests that the target callable can be given as a textual reference."""
    job._modify(func='tests.test_job:dummyfunc')
    assert job.func is dummyfunc
    assert job.func_ref == 'tests.test_job:dummyfunc'


def test_private_modify_unreachable_func(job):
    """Tests that func_ref remains None if no reference to the target callable can be found."""
    func = partial(dummyfunc)
    job._modify(func=func)
    assert job.func is func
    assert job.func_ref is None


def test_private_modify_update_name(job):
    """Tests that the name attribute defaults to the function name."""
    del job.name
    job._modify(func=dummyfunc)
    assert job.name == 'dummyfunc'


def test_private_modify_bad_args(job):
    """ Tests that passing an argument list of the wrong type raises a TypeError."""
    exc = pytest.raises(TypeError, job._modify, args=1)
    assert str(exc.value) == 'args must be a non-string iterable'


def test_private_modify_bad_kwargs(job):
    """Tests that passing an argument list of the wrong type raises a TypeError."""
    exc = pytest.raises(TypeError, job._modify, kwargs=1)
    assert str(exc.value) == 'kwargs must be a dict-like object'


@pytest.mark.parametrize('value', [1, ''], ids=['integer', 'empty string'])
def test_private_modify_bad_name(job, value):
    """
    Tests that passing an empty name or a name of something else than a string raises a TypeError.

    """
    exc = pytest.raises(TypeError, job._modify, name=value)
    assert str(exc.value) == 'name must be a nonempty string'


@pytest.mark.parametrize('value', ['foo', 0, -1], ids=['string', 'zero', 'negative'])
def test_private_modify_bad_misfire_grace_time(job, value):
    """Tests that passing a misfire_grace_time of the wrong type raises a TypeError."""
    exc = pytest.raises(TypeError, job._modify, misfire_grace_time=value)
    assert str(exc.value) == 'misfire_grace_time must be either None or a positive integer'


@pytest.mark.parametrize('value', [None, 'foo', 0, -1], ids=['None', 'string', 'zero', 'negative'])
def test_private_modify_bad_max_instances(job, value):
    """Tests that passing a max_instances of the wrong type raises a TypeError."""
    exc = pytest.raises(TypeError, job._modify, max_instances=value)
    assert str(exc.value) == 'max_instances must be a positive integer'


def test_private_modify_bad_trigger(job):
    """Tests that passing a trigger of the wrong type raises a TypeError."""
    exc = pytest.raises(TypeError, job._modify, trigger='foo')
    assert str(exc.value) == 'Expected a trigger instance, got str instead'


def test_private_modify_bad_executor(job):
    """Tests that passing an executor of the wrong type raises a TypeError."""
    exc = pytest.raises(TypeError, job._modify, executor=1)
    assert str(exc.value) == 'executor must be a string'


def test_private_modify_bad_next_run_time(job):
    """Tests that passing a next_run_time of the wrong type raises a TypeError."""
    exc = pytest.raises(TypeError, job._modify, next_run_time=1)
    assert str(exc.value) == 'Unsupported type for next_run_time: int'


def test_private_modify_bad_argument(job):
    """Tests that passing an unmodifiable argument type raises an AttributeError."""
    exc = pytest.raises(AttributeError, job._modify, scheduler=1)
    assert str(exc.value) == 'The following are not modifiable attributes of Job: scheduler'


def test_getstate(job):
    state = job.__getstate__()
    assert state == dict(
        version=1, trigger=job.trigger, executor='default', func='tests.test_job:dummyfunc',
        name=b'n\xc3\xa4m\xc3\xa9'.decode('utf-8'), args=(), kwargs={},
        id=b't\xc3\xa9st\xc3\xafd'.decode('utf-8'), misfire_grace_time=1, coalesce=False,
        max_instances=1, next_run_time=None)


def test_setstate(job, timezone):
    trigger = DateTrigger('2010-12-14 13:05:00', timezone)
    state = dict(
        version=1, scheduler=MagicMock(), jobstore=MagicMock(), trigger=trigger,
        executor='dummyexecutor', func='tests.test_job:dummyfunc', name='testjob.dummyfunc',
        args=[], kwargs={}, id='other_id', misfire_grace_time=2, coalesce=True, max_instances=2,
        next_run_time=None)
    job.__setstate__(state)
    assert job.id == 'other_id'
    assert job.func == dummyfunc
    assert job.func_ref == 'tests.test_job:dummyfunc'
    assert job.trigger == trigger
    assert job.executor == 'dummyexecutor'
    assert job.args == []
    assert job.kwargs == {}
    assert job.name == 'testjob.dummyfunc'
    assert job.misfire_grace_time == 2
    assert job.coalesce is True
    assert job.max_instances == 2
    assert job.next_run_time is None


def test_setstate_bad_version(job):
    """Tests that __setstate__ rejects state of higher version that it was designed to handle."""
    exc = pytest.raises(ValueError, job.__setstate__, {'version': 9999})
    assert 'Job has version 9999, but only version' in str(exc.value)


def test_eq(create_job):
    job = create_job(func=lambda: None, id='foo')
    job2 = create_job(func=lambda: None, id='foo')
    job3 = create_job(func=lambda: None, id='bar')
    assert job == job2
    assert not job == job3
    assert not job == 'foo'


def test_repr(job):
    if six.PY2:
        assert repr(job) == '<Job (id=t\\xe9st\\xefd name=n\\xe4m\\xe9)>'
    else:
        assert repr(job) == \
            b'<Job (id=t\xc3\xa9st\xc3\xafd name=n\xc3\xa4m\xc3\xa9)>'.decode('utf-8')


@pytest.mark.parametrize('status, expected_status', [
    ('scheduled', 'next run at: 2011-04-03 18:40:00 CEST'),
    ('paused', 'paused'),
    ('pending', 'pending')
], ids=['scheduled', 'paused', 'pending'])
@pytest.mark.parametrize('unicode', [False, True], ids=['nativestr', 'unicode'])
def test_str(create_job, status, unicode, expected_status):
    job = create_job(func=dummyfunc)
    if status == 'scheduled':
        job.next_run_time = job.trigger.run_date
    elif status == 'pending':
        del job.next_run_time

    if six.PY2 and not unicode:
        expected = 'n\\xe4m\\xe9 (trigger: date[2011-04-03 18:40:00 CEST], %s)' % expected_status
    else:
        expected = b'n\xc3\xa4m\xc3\xa9 (trigger: date[2011-04-03 18:40:00 CEST], %s)'.\
            decode('utf-8') % expected_status

    result = job.__unicode__() if unicode else job.__str__()
    assert result == expected
