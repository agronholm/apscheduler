from __future__ import annotations

from datetime import date, datetime, time
from zoneinfo import ZoneInfo

import pytest

from apscheduler.abc import Serializer
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


@pytest.mark.parametrize(
    "years, months, weeks, days, start_date, expected_dates",
    [
        pytest.param(
            0,
            0,
            0,
            1,
            date(2024, 3, 29),
            (date(2024, 3, 29), date(2024, 3, 31), date(2024, 4, 1)),
            id="daily",
        ),
        pytest.param(
            0,
            0,
            0,
            1,
            date(2024, 3, 30),
            (date(2024, 3, 31),),
            id="start-in-gap-end-on-next-day",
        ),
        pytest.param(
            0,
            0,
            0,
            2,
            date(2024, 3, 28),
            (date(2024, 3, 28), date(2024, 4, 1), date(2024, 4, 3)),
            id="every-other-day",
        ),
        pytest.param(
            0,
            0,
            1,
            0,
            date(2024, 3, 23),
            (date(2024, 3, 23), date(2024, 4, 6), date(2024, 4, 13)),
            id="weekly",
        ),
        pytest.param(
            0,
            1,
            0,
            0,
            date(2024, 1, 30),
            (date(2024, 1, 30), date(2024, 4, 30), date(2024, 5, 30)),
            id="monthly",
        ),
        pytest.param(
            1,
            0,
            0,
            0,
            date(2023, 3, 30),
            (date(2023, 3, 30), date(2025, 3, 30), date(2026, 3, 30)),
            id="yearly",
        ),
    ],
)
def test_missing_time_crossing_midnight(
    years: int,
    months: int,
    weeks: int,
    days: int,
    start_date: date,
    expected_dates: tuple[date, ...],
    serializer: Serializer | None,
) -> None:
    # Nuuk's missing 2024-03-30 23:30 normalizes to 2024-03-31 00:30.
    timezone = ZoneInfo("America/Nuuk")
    trigger = CalendarIntervalTrigger(
        years=years,
        months=months,
        weeks=weeks,
        days=days,
        hour=23,
        minute=30,
        start_date=start_date,
        end_date=expected_dates[-1],
        timezone=timezone,
    )
    for expected_date in expected_dates:
        if serializer:
            trigger = serializer.deserialize(serializer.serialize(trigger))

        assert trigger.next() == datetime.combine(expected_date, time(23, 30), timezone)

    assert trigger.next() is None


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


def test_utc_timezone(utc_timezone):
    trigger = CalendarIntervalTrigger(
        days=1, hour=1, start_date=date(2016, 3, 31), timezone=utc_timezone
    )
    assert trigger.next() == datetime(2016, 3, 31, 1, tzinfo=utc_timezone)
