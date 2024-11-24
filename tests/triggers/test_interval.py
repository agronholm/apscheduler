import pickle
import sys
from datetime import date, datetime, timedelta
from unittest.mock import Mock

import pytest
import pytz

from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.util import localize

if sys.version_info < (3, 9):
    from backports.zoneinfo import ZoneInfo
else:
    from zoneinfo import ZoneInfo


@pytest.fixture()
def trigger(timezone):
    return IntervalTrigger(
        seconds=1, start_date=datetime(2009, 8, 4, second=2), timezone=timezone
    )


def test_invalid_interval(timezone):
    pytest.raises(TypeError, IntervalTrigger, "1-6", timezone=timezone)


def test_start_end_times_string(timezone, monkeypatch):
    monkeypatch.setattr(
        "apscheduler.triggers.interval.get_localzone", Mock(return_value=timezone)
    )
    trigger = IntervalTrigger(
        start_date="2016-11-05 05:06:53", end_date="2017-11-05 05:11:32"
    )
    assert trigger.start_date == localize(datetime(2016, 11, 5, 5, 6, 53), timezone)
    assert trigger.end_date == localize(datetime(2017, 11, 5, 5, 11, 32), timezone)


def test_before(trigger, timezone):
    """Tests that if "start_date" is later than "now", it will return start_date."""
    now = trigger.start_date - timedelta(seconds=2)
    assert trigger.get_next_fire_time(None, now) == trigger.start_date


def test_within(trigger, timezone):
    """
    Tests that if "now" is between "start_date" and the next interval, it will return the next
    interval.

    """
    now = trigger.start_date + timedelta(microseconds=1000)
    assert (
        trigger.get_next_fire_time(None, now) == trigger.start_date + trigger.interval
    )


def test_no_start_date(timezone):
    trigger = IntervalTrigger(seconds=2, timezone=timezone)
    now = datetime.now(timezone)
    assert (trigger.get_next_fire_time(None, now) - now) <= timedelta(seconds=2)


def test_different_tz(trigger, timezone):
    alter_tz = pytz.FixedOffset(-60)
    start_date = alter_tz.localize(datetime(2009, 8, 3, 22, second=2, microsecond=1000))
    correct_next_date = localize(datetime(2009, 8, 4, 1, second=3), timezone)
    assert trigger.get_next_fire_time(None, start_date) == correct_next_date


def test_end_date(timezone):
    """Tests that the interval trigger won't return any datetimes past the set end time."""
    start_date = localize(datetime(2014, 5, 26), timezone)
    trigger = IntervalTrigger(
        minutes=5,
        start_date=start_date,
        end_date=datetime(2014, 5, 26, 0, 7),
        timezone=timezone,
    )
    assert trigger.get_next_fire_time(
        None, start_date + timedelta(minutes=2)
    ) == start_date.replace(minute=5)
    assert trigger.get_next_fire_time(None, start_date + timedelta(minutes=6)) is None


def test_dst_change():
    """
    Making sure that IntervalTrigger works during the ambiguous "fall-back" DST period.
    Note that you should explicitly compare datetimes as strings to avoid the internal datetime
    comparison which would test for equality in the UTC timezone.

    """
    eastern = ZoneInfo("US/Eastern")
    start_date = datetime(2013, 3, 1)  # Start within EDT
    trigger = IntervalTrigger(hours=1, start_date=start_date, timezone=eastern)

    datetime_edt = datetime(2013, 3, 10, 1, 5, tzinfo=eastern)
    correct_next_date = datetime(2013, 3, 10, 3, tzinfo=eastern)
    assert str(trigger.get_next_fire_time(None, datetime_edt)) == str(correct_next_date)

    datetime_est = datetime(2013, 11, 3, 1, 5, tzinfo=eastern)
    correct_next_date = datetime(2013, 11, 3, 1, tzinfo=eastern, fold=1)
    next_fire_time = trigger.get_next_fire_time(None, datetime_est)
    assert next_fire_time == correct_next_date
    assert str(next_fire_time) == str(correct_next_date)


def test_space_in_expr(timezone):
    trigger = CronTrigger(day="1-2, 4-7", timezone=timezone)
    assert repr(trigger) == "<CronTrigger (day='1-2,4-7', timezone='Europe/Berlin')>"


def test_repr(trigger):
    assert repr(trigger) == (
        "<IntervalTrigger (interval=datetime.timedelta(seconds=1), "
        "start_date='2009-08-04 00:00:02 CEST', "
        "timezone='Europe/Berlin')>"
    )


def test_str(trigger):
    assert str(trigger) == "interval[0:00:01]"


def test_pickle(timezone):
    """Test that the trigger is pickleable."""

    trigger = IntervalTrigger(
        weeks=2,
        days=6,
        minutes=13,
        seconds=2,
        start_date=date(2016, 4, 3),
        timezone=timezone,
        jitter=12,
    )
    data = pickle.dumps(trigger, 2)
    trigger2 = pickle.loads(data)

    for attr in IntervalTrigger.__slots__:
        assert getattr(trigger2, attr) == getattr(trigger, attr)


def test_jitter_produces_different_valid_results(timezone):
    trigger = IntervalTrigger(seconds=5, timezone=timezone, jitter=3)
    now = datetime.now(timezone)

    results = set()
    for _ in range(0, 100):
        next_fire_time = trigger.get_next_fire_time(None, now)
        results.add(next_fire_time)
        assert timedelta(seconds=2) <= (next_fire_time - now) <= timedelta(seconds=8)
    assert 1 < len(results)


@pytest.mark.parametrize(
    "trigger_args, start_date, start_date_dst, correct_next_date",
    [
        (
            {"hours": 1},
            datetime(2013, 3, 10, 1, 35),
            False,
            datetime(2013, 3, 10, 3, 35),
        ),
        (
            {"hours": 1},
            datetime(2013, 11, 3, 1, 35),
            True,
            datetime(2013, 11, 3, 1, 35),
        ),
    ],
    ids=["interval_spring", "interval_autumn"],
)
def test_jitter_dst_change(trigger_args, start_date, start_date_dst, correct_next_date):
    timezone = pytz.timezone("US/Eastern")
    epsilon = timedelta(seconds=1)
    start_date = timezone.localize(start_date, is_dst=start_date_dst)
    trigger = IntervalTrigger(
        timezone=timezone, start_date=start_date, jitter=5, **trigger_args
    )
    correct_next_date = timezone.localize(correct_next_date, is_dst=not start_date_dst)

    for _ in range(0, 100):
        next_fire_time = trigger.get_next_fire_time(None, start_date + epsilon)
        assert abs(next_fire_time - correct_next_date) <= timedelta(seconds=5)


def test_jitter_with_end_date(timezone):
    now = localize(datetime(2017, 11, 12, 6, 55, 58), timezone)
    end_date = localize(datetime(2017, 11, 12, 6, 56, 0), timezone)
    trigger = IntervalTrigger(seconds=5, jitter=5, end_date=end_date)

    for _ in range(0, 100):
        next_fire_time = trigger.get_next_fire_time(None, now)
        assert next_fire_time is None or next_fire_time <= end_date
