from __future__ import annotations

from datetime import date, datetime

import pytest

from apscheduler.triggers.calendarinterval import CalendarIntervalTrigger


def test_bad_interval(timezone):
    exc = pytest.raises(ValueError, CalendarIntervalTrigger, timezone=timezone)
    exc.match("interval must be at least 1 day long")


def test_bad_start_end_dates(timezone):
    exc = pytest.raises(
        ValueError,
        CalendarIntervalTrigger,
        days=1,
        start_date=date(2016, 3, 4),
        end_date=date(2016, 3, 3),
        timezone=timezone,
    )
    exc.match("end_date cannot be earlier than start_date")


def test_end_date(timezone, serializer):
    """Test that end_date is respected."""
    start_end_date = date(2020, 12, 31)
    trigger = CalendarIntervalTrigger(
        days=1, start_date=start_end_date, end_date=start_end_date, timezone=timezone
    )
    if serializer:
        trigger = serializer.deserialize(serializer.serialize(trigger))

    assert trigger.next().date() == start_end_date
    assert trigger.next() is None


def test_missing_time(timezone, serializer):
    """
    Test that if the designated time does not exist on a day due to a forward DST shift,
    the day is skipped entirely.

    """
    trigger = CalendarIntervalTrigger(
        days=1, hour=2, minute=30, start_date=date(2016, 3, 27), timezone=timezone
    )
    if serializer:
        trigger = serializer.deserialize(serializer.serialize(trigger))

    assert trigger.next() == datetime(2016, 3, 28, 2, 30, tzinfo=timezone)


def test_repeated_time(timezone, serializer):
    """
    Test that if the designated time is repeated during a day due to a backward DST
    shift, the task is executed on the earlier occurrence of that time.

    """
    trigger = CalendarIntervalTrigger(
        days=2, hour=2, minute=30, start_date=date(2016, 10, 30), timezone=timezone
    )
    if serializer:
        trigger = serializer.deserialize(serializer.serialize(trigger))

    assert trigger.next() == datetime(2016, 10, 30, 2, 30, tzinfo=timezone, fold=0)


def test_nonexistent_days(timezone, serializer):
    """Test that invalid dates are skipped."""
    trigger = CalendarIntervalTrigger(
        months=1, start_date=date(2016, 3, 31), timezone=timezone
    )
    assert trigger.next() == datetime(2016, 3, 31, tzinfo=timezone)

    if serializer:
        trigger = serializer.deserialize(serializer.serialize(trigger))

    assert trigger.next() == datetime(2016, 5, 31, tzinfo=timezone)


def test_repr(timezone, serializer):
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
    if serializer:
        trigger = serializer.deserialize(serializer.serialize(trigger))

    assert repr(trigger) == (
        "CalendarIntervalTrigger(years=1, months=5, weeks=6, days=8, "
        "time='03:00:08', start_date='2016-03-05', end_date='2020-12-25', "
        "timezone='Europe/Berlin')"
    )
