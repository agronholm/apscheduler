from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from apscheduler.triggers.interval import IntervalTrigger


def test_bad_interval():
    exc = pytest.raises(ValueError, IntervalTrigger)
    exc.match("The time interval must be positive")


def test_bad_end_time(timezone):
    start_time = datetime(2020, 5, 16, tzinfo=timezone)
    end_time = datetime(2020, 5, 15, tzinfo=timezone)
    exc = pytest.raises(
        ValueError, IntervalTrigger, seconds=1, start_time=start_time, end_time=end_time
    )
    exc.match("end_time cannot be earlier than start_time")


def test_end_time(timezone, serializer):
    start_time = datetime(2020, 5, 16, 19, 32, 44, 649521, tzinfo=timezone)
    end_time = datetime(2020, 5, 16, 22, 33, 1, tzinfo=timezone)
    interval = timedelta(hours=1, seconds=6)
    trigger = IntervalTrigger(
        start_time=start_time, end_time=end_time, hours=1, seconds=6
    )
    assert trigger.next() == start_time

    if serializer:
        trigger = serializer.deserialize(serializer.serialize(trigger))

    assert trigger.next() == start_time + interval
    assert trigger.next() == start_time + interval * 2
    assert trigger.next() is None


def test_repr(timezone, serializer):
    start_time = datetime(2020, 5, 15, 12, 55, 32, 954032, tzinfo=timezone)
    end_time = datetime(2020, 6, 4, 16, 18, 49, 306942, tzinfo=timezone)
    trigger = IntervalTrigger(
        weeks=1,
        days=2,
        hours=3,
        minutes=4,
        seconds=5,
        microseconds=123525,
        start_time=start_time,
        end_time=end_time,
    )
    if serializer:
        trigger = serializer.deserialize(serializer.serialize(trigger))

    assert repr(trigger) == (
        "IntervalTrigger(weeks=1, days=2, hours=3, minutes=4, seconds=5, "
        "microseconds=123525, start_time='2020-05-15 12:55:32.954032+02:00', "
        "end_time='2020-06-04 16:18:49.306942+02:00')"
    )


def test_datetime_max_boundary_returns_none():
    start_time = datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC)
    trigger = IntervalTrigger(seconds=1, start_time=start_time)

    assert trigger.next() == start_time
    assert trigger.next() is None
    # Once exhausted, subsequent calls keep returning None without raising.
    assert trigger.next() is None


def test_normal_interval_sequence_unaffected():
    start_time = datetime(2020, 5, 16, tzinfo=UTC)
    trigger = IntervalTrigger(seconds=1, start_time=start_time)

    assert trigger.next() == start_time
    assert trigger.next() == start_time + timedelta(seconds=1)
    assert trigger.next() == start_time + timedelta(seconds=2)
