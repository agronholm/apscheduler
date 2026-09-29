from __future__ import annotations

from datetime import datetime, timedelta
from datetime import timezone as datetime_timezone

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


@pytest.mark.parametrize(
    "start_time",
    [
        datetime(2026, 3, 29, 1, 59, 59, 123456),
        datetime(2026, 10, 25, 2, 59, 59, 123456),
        datetime(2026, 10, 25, 2, 30, 0, 123456, fold=1),
    ],
    ids=["spring-forward", "fall-back", "second-fold"],
)
def test_elapsed_interval_across_dst(start_time, timezone):
    start_time = start_time.replace(tzinfo=timezone)
    trigger = IntervalTrigger(seconds=1, start_time=start_time)
    assert trigger.next() == start_time

    for seconds in (1, 2):
        expected = start_time.astimezone(datetime_timezone.utc) + timedelta(
            seconds=seconds
        )
        result = trigger.next()
        assert result.astimezone(datetime_timezone.utc) == expected
        assert result.isoformat() == expected.astimezone(timezone).isoformat()


@pytest.mark.parametrize("start_fold,end_fold", [(0, 1), (1, 0)])
def test_end_time_validation_across_dst(start_fold, end_fold, timezone):
    start_time = datetime(
        2026, 10, 25, 2, 45 if start_fold == 0 else 15, tzinfo=timezone, fold=start_fold
    )
    end_time = datetime(
        2026, 10, 25, 2, 15 if start_fold == 0 else 45, tzinfo=timezone, fold=end_fold
    )
    if start_fold == 1:
        # A later wall time in the first fold is earlier in UTC.
        with pytest.raises(ValueError, match="end_time cannot be earlier"):
            IntervalTrigger(minutes=15, start_time=start_time, end_time=end_time)
    else:
        trigger = IntervalTrigger(minutes=15, start_time=start_time, end_time=end_time)
        assert trigger.next() == start_time
        assert trigger.next().astimezone(datetime_timezone.utc) == datetime(
            2026, 10, 25, 1, tzinfo=datetime_timezone.utc
        )
        assert trigger.next().astimezone(datetime_timezone.utc) == end_time.astimezone(
            datetime_timezone.utc
        )
        assert trigger.next() is None


def test_end_time_does_not_allow_later_fold(timezone):
    start_time = datetime(2026, 10, 25, 2, 30, tzinfo=timezone)
    trigger = IntervalTrigger(hours=1, start_time=start_time, end_time=start_time)
    assert trigger.next() == start_time
    assert trigger.next() is None
