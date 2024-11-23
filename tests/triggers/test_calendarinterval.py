import pickle
import sys
from datetime import date, datetime
from datetime import timezone as timezone_type

import pytest

from apscheduler.triggers.calendarinterval import CalendarIntervalTrigger
from apscheduler.util import astimezone, localize

if sys.version_info < (3, 9):
    from backports.zoneinfo import ZoneInfo
else:
    from zoneinfo import ZoneInfo


def test_bad_interval(timezone: ZoneInfo) -> None:
    exc = pytest.raises(ValueError, CalendarIntervalTrigger, timezone=timezone)
    exc.match("interval must be at least 1 day long")


def test_bad_start_end_dates(timezone: ZoneInfo) -> None:
    exc = pytest.raises(
        ValueError,
        CalendarIntervalTrigger,
        days=1,
        start_date=date(2016, 3, 4),
        end_date=date(2016, 3, 3),
        timezone=timezone,
    )
    exc.match("end_date cannot be earlier than start_date")


def test_end_date(timezone: ZoneInfo) -> None:
    """Test that end_date is respected."""
    start_end_date = date(2020, 12, 31)
    trigger = CalendarIntervalTrigger(
        days=1, start_date=start_end_date, end_date=start_end_date, timezone=timezone
    )
    trigger = pickle.loads(pickle.dumps(trigger, protocol=pickle.HIGHEST_PROTOCOL))

    now = datetime.now(astimezone(timezone))
    next_fire_time = trigger.get_next_fire_time(None, now)
    assert next_fire_time.date() == start_end_date
    assert trigger.get_next_fire_time(next_fire_time, now) is None


def test_missing_time(timezone: ZoneInfo) -> None:
    """
    Test that if the designated time does not exist on a day due to a forward DST shift,
    the day is skipped entirely.

    """
    trigger = CalendarIntervalTrigger(
        days=1, hour=2, minute=30, start_date=date(2016, 3, 27), timezone=timezone
    )
    trigger = pickle.loads(pickle.dumps(trigger, protocol=pickle.HIGHEST_PROTOCOL))
    now = datetime.now(astimezone(timezone))
    assert trigger.get_next_fire_time(None, now) == localize(
        datetime(2016, 3, 28, 2, 30), timezone
    )


def test_repeated_time(timezone: ZoneInfo) -> None:
    """
    Test that if the designated time is repeated during a day due to a backward DST
    shift, the task is executed on the earlier occurrence of that time.

    """
    trigger = CalendarIntervalTrigger(
        days=2, hour=2, minute=30, start_date=date(2016, 10, 30), timezone=timezone
    )
    trigger = pickle.loads(pickle.dumps(trigger, protocol=pickle.HIGHEST_PROTOCOL))
    now = datetime.now(astimezone(timezone))
    assert trigger.get_next_fire_time(None, now) == datetime(
        2016, 10, 30, 2, 30, tzinfo=astimezone(timezone), fold=0
    )


def test_nonexistent_days(timezone: ZoneInfo) -> None:
    """Test that invalid dates are skipped."""
    trigger = CalendarIntervalTrigger(
        months=1, start_date=date(2016, 3, 31), timezone=timezone
    )
    now = datetime.now(astimezone(timezone))
    next_fire_time = trigger.get_next_fire_time(None, now)
    assert next_fire_time == datetime(2016, 3, 31, tzinfo=astimezone(timezone))

    trigger = pickle.loads(pickle.dumps(trigger, protocol=pickle.HIGHEST_PROTOCOL))
    assert trigger.get_next_fire_time(next_fire_time, now) == datetime(
        2016, 5, 31, tzinfo=astimezone(timezone)
    )


def test_repr(timezone: ZoneInfo) -> None:
    trigger = CalendarIntervalTrigger(
        years=1,
        months=5,
        weeks=6,
        days=8,
        hour=3,
        second=8,
        start_date=date(2016, 3, 5),
        end_date=date(2020, 12, 25),
        timezone=timezone,
    )
    trigger = pickle.loads(pickle.dumps(trigger, protocol=pickle.HIGHEST_PROTOCOL))

    assert repr(trigger) == (
        "CalendarIntervalTrigger(years=1, months=5, weeks=6, days=8, "
        "time='03:00:08', start_date='2016-03-05', end_date='2020-12-25', "
        "timezone='Europe/Berlin')"
    )


def test_utc_timezone() -> None:
    trigger = CalendarIntervalTrigger(
        days=1, hour=1, start_date=date(2016, 3, 31), timezone=timezone_type.utc
    )
    now = datetime.now(timezone_type.utc)
    assert trigger.get_next_fire_time(None, now) == datetime(
        2016, 3, 31, 1, tzinfo=timezone_type.utc
    )
