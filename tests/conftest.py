import sys
from datetime import datetime
from unittest.mock import Mock

import pytest
from apscheduler.serializers.cbor import CBORSerializer
from apscheduler.serializers.json import JSONSerializer
from apscheduler.serializers.pickle import PickleSerializer

if sys.version_info >= (3, 9):
    from zoneinfo import ZoneInfo
else:
    from backports.zoneinfo import ZoneInfo


@pytest.fixture(scope='session')
def timezone():
    return ZoneInfo('Europe/Berlin')


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
    monkeypatch.setattr('apscheduler.triggers.interval.datetime', fake_datetime)
    monkeypatch.setattr('apscheduler.triggers.date.datetime', fake_datetime)
    return freezer


@pytest.fixture(params=[None, PickleSerializer, CBORSerializer, JSONSerializer],
                ids=['none', 'pickle', 'cbor', 'json'])
def serializer(request):
    return request.param() if request.param else None
