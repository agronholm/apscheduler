from datetime import datetime
import sys

from dateutil.tz import tzoffset
import pytest

from apscheduler.job import Job

try:
    from unittest.mock import Mock
except ImportError:
    from mock import Mock


def minpython(*version):
    version_str = '.'.join([str(num) for num in version])

    def outer(func):
        dec = pytest.mark.skipif(sys.version_info < version, reason='Requires Python >= %s' % version_str)
        return dec(func)
    return outer


def maxpython(*version):
    version_str = '.'.join([str(num) for num in version])

    def outer(func):
        dec = pytest.mark.skipif(sys.version_info >= version, reason='Requires Python < %s' % version_str)
        return dec(func)
    return outer


@pytest.fixture(scope='session')
def timezone():
    return tzoffset('DUMMYTZ', 3600)


@pytest.fixture
def freeze_time(monkeypatch, timezone):
    class TimeFreezer:
        def __init__(self, initial):
            self.current = initial
            self.increment = None

        def get(self, tzinfo):
            now = self.current.astimezone(tzinfo)
            if self.increment:
                self.current += self.increment
            return now

        def set(self, new_time):
            self.current = new_time

        def next(self,):
            return self.current + self.increment

        def set_increment(self, delta):
            self.increment = delta

    freezer = TimeFreezer(datetime(2011, 4, 3, 18, 40, tzinfo=timezone))
    fake_datetime = Mock(datetime, now=freezer.get)
    monkeypatch.setattr('apscheduler.schedulers.base.datetime', fake_datetime)
    monkeypatch.setattr('apscheduler.executors.base.datetime', fake_datetime)
    return freezer


@pytest.fixture(scope='session')
def job_defaults(timezone):
    run_date = datetime(2011, 4, 3, 18, 40, tzinfo=timezone)
    return {'trigger': 'date', 'trigger_args': {'run_date': run_date, 'timezone': timezone}, 'executor': 'default',
            'args': (), 'kwargs': {}, 'id': 'testid', 'misfire_grace_time': 1, 'coalesce': False, 'name': None,
            'max_runs': None, 'max_instances': 1}


@pytest.fixture(scope='session')
def create_job(job_defaults):
    def create(**kwargs):
        job_kwargs = job_defaults.copy()
        job_kwargs.update(kwargs)
        return Job(**job_kwargs)
    return create
