# coding: utf-8
from datetime import datetime
import sys

import pytest
import pytz

from apscheduler.job import Job
from apscheduler.schedulers.base import BaseScheduler
from apscheduler.schedulers.blocking import BlockingScheduler


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
    return pytz.timezone('Europe/Berlin')


@pytest.fixture
def freeze_time(monkeypatch, timezone):
    class TimeFreezer:
        def __init__(self, initial):
            self.current = initial
            self.increment = None

        def get(self, tzinfo=None):
            now = self.current.astimezone(tzinfo) if tzinfo else self.current.replace(tzinfo=None)
            if self.increment:
                self.current += self.increment
            return now

        def set(self, new_time):
            self.current = new_time

        def next(self,):
            return self.current + self.increment

        def set_increment(self, delta):
            self.increment = delta

    freezer = TimeFreezer(timezone.localize(datetime(2011, 4, 3, 18, 40)))
    fake_datetime = Mock(datetime, now=freezer.get)
    monkeypatch.setattr('apscheduler.schedulers.base.datetime', fake_datetime)
    monkeypatch.setattr('apscheduler.executors.base.datetime', fake_datetime)
    monkeypatch.setattr('apscheduler.triggers.interval.datetime', fake_datetime)
    monkeypatch.setattr('apscheduler.triggers.date.datetime', fake_datetime)
    return freezer


@pytest.fixture(scope='session')
def job_defaults(timezone):
    run_date = timezone.localize(datetime(2011, 4, 3, 18, 40))
    return {'trigger': 'date', 'trigger_args': {'run_date': run_date, 'timezone': timezone}, 'executor': 'default',
            'args': (), 'kwargs': {}, 'id': b't\xc3\xa9st\xc3\xafd'.decode('utf-8'), 'misfire_grace_time': 1,
            'coalesce': False, 'name': b'n\xc3\xa4m\xc3\xa9'.decode('utf-8'), 'max_instances': 1}


@pytest.fixture(scope='session')
def create_job(job_defaults, timezone):
    def create(**kwargs):
        kwargs.setdefault('scheduler', Mock(BaseScheduler, timezone=timezone))
        job_kwargs = job_defaults.copy()
        job_kwargs.update(kwargs)
        job_kwargs['trigger'] = BlockingScheduler()._create_trigger(job_kwargs.pop('trigger'),
                                                                    job_kwargs.pop('trigger_args'))
        job_kwargs.setdefault('next_run_time', None)
        return Job(**job_kwargs)
    return create
