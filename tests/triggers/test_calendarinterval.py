import pickle
from datetime import datetime, date

import pytest
from pytz.tzinfo import DstTzInfo

from apscheduler.triggers.calendarinterval import CalendarIntervalTrigger


@pytest.fixture
def trigger(timezone: DstTzInfo):
    return CalendarIntervalTrigger(
        years=1, months=5, weeks=6, days=8, hour=3, second=8, start_date=date(2016, 3, 5),
        end_date=date(2020, 12, 25), timezone=timezone)


def test_bad_interval(timezone: DstTzInfo):
    exc = pytest.raises(ValueError, CalendarIntervalTrigger, timezone=timezone)
    exc.match('interval must be at least 1 day long')


def test_bad_start_end_dates(timezone: DstTzInfo):
    exc = pytest.raises(ValueError, CalendarIntervalTrigger, days=1,
                        start_date=date(2016, 3, 4), end_date=date(2016, 3, 3), timezone=timezone)
    exc.match('end_date cannot be earlier than start_date')


def test_pickle(trigger: CalendarIntervalTrigger):
    payload = pickle.dumps(trigger)
    deserialized = pickle.loads(payload)

    for attr in CalendarIntervalTrigger.__slots__:
        assert getattr(deserialized, attr) == getattr(trigger, attr)


def test_setstate_unhandled_version(trigger: CalendarIntervalTrigger):
    exc = pytest.raises(ValueError, trigger.__setstate__, {'version': 2})
    exc.match('Got serialized data for version 2 of CalendarIntervalTrigger, '
              'but only version 1 can be handled')


def test_start_date(trigger: CalendarIntervalTrigger, timezone: DstTzInfo):
    """Test that start_date is respected."""
    now = timezone.localize(datetime(2016, 1, 15))
    expected = timezone.localize(datetime(2016, 3, 5, 3, 0, 8))
    assert trigger.get_next_fire_time(None, now) == expected


def test_end_date(trigger: CalendarIntervalTrigger, timezone: DstTzInfo):
    """Test that end_date is respected."""
    now = timezone.localize(datetime(2020, 12, 31))
    previous = timezone.localize(datetime(2020, 12, 25))
    assert trigger.get_next_fire_time(now, previous) is None


def test_missing_time(timezone: DstTzInfo):
    """
    Test that if the designated time does not exist on a day due to a forward DST shift, the day is
    skipped entirely.

    """
    trigger = CalendarIntervalTrigger(days=1, hour=2, minute=30)
    now = timezone.localize(datetime(2016, 3, 27))
    expected = timezone.localize(datetime(2016, 3, 28, 2, 30))
    assert trigger.get_next_fire_time(None, now) == expected


def test_repeated_time(timezone: DstTzInfo):
    """
    Test that if the designated time is repeated during a day due to a backward DST shift, the task
    is executed on the earlier occurrence of that time.

    """
    trigger = CalendarIntervalTrigger(days=2, hour=2, minute=30)
    now = timezone.localize(datetime(2016, 10, 30))
    expected = timezone.localize(datetime(2016, 10, 30, 2, 30), is_dst=True)
    assert trigger.get_next_fire_time(None, now) == expected


def test_nonexistent_days(timezone: DstTzInfo):
    """Test that invalid dates are skipped."""
    trigger = CalendarIntervalTrigger(months=1)
    now = timezone.localize(datetime(2016, 4, 30))
    previous = timezone.localize(datetime(2016, 3, 31))
    expected = timezone.localize(datetime(2016, 5, 31))
    assert trigger.get_next_fire_time(previous, now) == expected


def test_str(trigger):
    assert str(trigger) == 'calendarinterval[1y, 5m, 6w, 8d at 03:00:08]'


def test_repr(trigger):
    assert repr(trigger) == ("CalendarIntervalTrigger(years=1, months=5, weeks=6, days=8, "
                             "time=03:00:08)")
