import pickle
import sys
from datetime import datetime, timedelta
from unittest.mock import Mock

import pytest
import pytz

from apscheduler.triggers.cron import CronTrigger
from apscheduler.util import localize

if sys.version_info < (3, 9):
    from backports.zoneinfo import ZoneInfo
else:
    from zoneinfo import ZoneInfo


def test_cron_trigger_1(timezone):
    trigger = CronTrigger(year="2009/2", month="1/3", day="5-13", timezone=timezone)
    assert repr(trigger) == (
        "<CronTrigger (year='2009/2', month='1/3', day='5-13', "
        "timezone='Europe/Berlin')>"
    )
    assert str(trigger) == "cron[year='2009/2', month='1/3', day='5-13']"
    start_date = localize(datetime(2008, 12, 1), timezone)
    correct_next_date = localize(datetime(2009, 1, 5), timezone)
    assert trigger.get_next_fire_time(None, start_date) == correct_next_date


def test_cron_trigger_2(timezone):
    trigger = CronTrigger(year="2009/2", month="1/3", day="5-13", timezone=timezone)
    start_date = localize(datetime(2009, 10, 14), timezone)
    correct_next_date = localize(datetime(2011, 1, 5), timezone)
    assert trigger.get_next_fire_time(None, start_date) == correct_next_date


def test_cron_trigger_3(timezone):
    trigger = CronTrigger(year="2009", month="feb-dec", hour="8-10", timezone=timezone)
    assert repr(trigger) == (
        "<CronTrigger (year='2009', month='feb-dec', hour='8-10', "
        "timezone='Europe/Berlin')>"
    )
    start_date = localize(datetime(2009, 1, 1), timezone)
    correct_next_date = localize(datetime(2009, 2, 1, 8), timezone)
    assert trigger.get_next_fire_time(None, start_date) == correct_next_date


def test_cron_trigger_4(timezone):
    trigger = CronTrigger(year="2012", month="2", day="last", timezone=timezone)
    assert repr(trigger) == (
        "<CronTrigger (year='2012', month='2', day='last', "
        "timezone='Europe/Berlin')>"
    )
    start_date = localize(datetime(2012, 2, 1), timezone)
    correct_next_date = localize(datetime(2012, 2, 29), timezone)
    assert trigger.get_next_fire_time(None, start_date) == correct_next_date


def test_start_end_times_string(timezone, monkeypatch):
    monkeypatch.setattr(
        "apscheduler.triggers.cron.get_localzone", Mock(return_value=timezone)
    )
    trigger = CronTrigger(
        start_date="2016-11-05 05:06:53", end_date="2017-11-05 05:11:32"
    )
    assert trigger.start_date == localize(datetime(2016, 11, 5, 5, 6, 53), timezone)
    assert trigger.end_date == localize(datetime(2017, 11, 5, 5, 11, 32), timezone)


def test_cron_zero_value(timezone):
    trigger = CronTrigger(year=2009, month=2, hour=0, timezone=timezone)
    assert repr(trigger) == (
        "<CronTrigger (year='2009', month='2', hour='0', timezone='Europe/Berlin')>"
    )


def test_cron_year_list(timezone):
    trigger = CronTrigger(year="2009,2008", timezone=timezone)
    assert repr(trigger) == "<CronTrigger (year='2009,2008', timezone='Europe/Berlin')>"
    assert str(trigger) == "cron[year='2009,2008']"
    start_date = localize(datetime(2009, 1, 1), timezone)
    correct_next_date = localize(datetime(2009, 1, 1), timezone)
    assert trigger.get_next_fire_time(None, start_date) == correct_next_date


def test_cron_start_date(timezone):
    trigger = CronTrigger(
        year="2009",
        month="2",
        hour="8-10",
        start_date="2009-02-03 11:00:00",
        timezone=timezone,
    )
    assert repr(trigger) == (
        "<CronTrigger (year='2009', month='2', hour='8-10', "
        "start_date='2009-02-03 11:00:00 CET', "
        "timezone='Europe/Berlin')>"
    )
    assert str(trigger) == "cron[year='2009', month='2', hour='8-10']"
    start_date = localize(datetime(2009, 1, 1), timezone)
    correct_next_date = localize(datetime(2009, 2, 4, 8), timezone)
    assert trigger.get_next_fire_time(None, start_date) == correct_next_date


def test_previous_fire_time_1(timezone):
    """Test for previous_fire_time arg in get_next_fire_time()"""
    trigger = CronTrigger(day="*", timezone=timezone)
    previous_fire_time = localize(datetime(2015, 11, 23), timezone)
    now = localize(datetime(2015, 11, 26), timezone)
    correct_next_date = localize(datetime(2015, 11, 24), timezone)
    assert trigger.get_next_fire_time(previous_fire_time, now) == correct_next_date


def test_previous_fire_time_2(timezone):
    trigger = CronTrigger(day="*", timezone=timezone)
    previous_fire_time = localize(datetime(2015, 11, 23), timezone)
    now = localize(datetime(2015, 11, 22), timezone)
    correct_next_date = localize(datetime(2015, 11, 22), timezone)
    assert trigger.get_next_fire_time(previous_fire_time, now) == correct_next_date


def test_previous_fire_time_3(timezone):
    trigger = CronTrigger(day="*", timezone=timezone)
    previous_fire_time = localize(datetime(2016, 4, 25), timezone)
    now = localize(datetime(2016, 4, 25), timezone)
    correct_next_date = localize(datetime(2016, 4, 26), timezone)
    assert trigger.get_next_fire_time(previous_fire_time, now) == correct_next_date


def test_cron_weekday_overlap(timezone):
    trigger = CronTrigger(
        year=2009, month=1, day="6-10", day_of_week="2-4", timezone=timezone
    )
    assert repr(trigger) == (
        "<CronTrigger (year='2009', month='1', day='6-10', "
        "day_of_week='2-4', timezone='Europe/Berlin')>"
    )
    assert str(trigger) == "cron[year='2009', month='1', day='6-10', day_of_week='2-4']"
    start_date = localize(datetime(2009, 1, 1), timezone)
    correct_next_date = localize(datetime(2009, 1, 7), timezone)
    assert trigger.get_next_fire_time(None, start_date) == correct_next_date


def test_cron_weekday_nomatch(timezone):
    trigger = CronTrigger(
        year=2009, month=1, day="6-10", day_of_week="0,6", timezone=timezone
    )
    assert repr(trigger) == (
        "<CronTrigger (year='2009', month='1', day='6-10', "
        "day_of_week='0,6', timezone='Europe/Berlin')>"
    )
    assert str(trigger) == "cron[year='2009', month='1', day='6-10', day_of_week='0,6']"
    start_date = localize(datetime(2009, 1, 1), timezone)
    correct_next_date = None
    assert trigger.get_next_fire_time(None, start_date) == correct_next_date


def test_cron_weekday_positional(timezone):
    trigger = CronTrigger(year=2009, month=1, day="4th wed", timezone=timezone)
    assert repr(trigger) == (
        "<CronTrigger (year='2009', month='1', day='4th wed', "
        "timezone='Europe/Berlin')>"
    )
    assert str(trigger) == "cron[year='2009', month='1', day='4th wed']"
    start_date = localize(datetime(2009, 1, 1), timezone)
    correct_next_date = localize(datetime(2009, 1, 28), timezone)
    assert trigger.get_next_fire_time(None, start_date) == correct_next_date


def test_week_1(timezone):
    trigger = CronTrigger(year=2009, month=2, week=8, timezone=timezone)
    assert repr(trigger) == (
        "<CronTrigger (year='2009', month='2', week='8', timezone='Europe/Berlin')>"
    )
    assert str(trigger) == "cron[year='2009', month='2', week='8']"
    start_date = localize(datetime(2009, 1, 1), timezone)
    correct_next_date = localize(datetime(2009, 2, 16), timezone)
    assert trigger.get_next_fire_time(None, start_date) == correct_next_date


def test_week_2(timezone):
    trigger = CronTrigger(year=2009, week=15, day_of_week=2, timezone=timezone)
    assert repr(trigger) == (
        "<CronTrigger (year='2009', week='15', day_of_week='2', "
        "timezone='Europe/Berlin')>"
    )
    assert str(trigger) == "cron[year='2009', week='15', day_of_week='2']"
    start_date = localize(datetime(2009, 1, 1), timezone)
    correct_next_date = localize(datetime(2009, 4, 8), timezone)
    assert trigger.get_next_fire_time(None, start_date) == correct_next_date


def test_cron_extra_coverage(timezone):
    # This test has no value other than patching holes in test coverage
    trigger = CronTrigger(day="6,8", timezone=timezone)
    assert repr(trigger) == "<CronTrigger (day='6,8', timezone='Europe/Berlin')>"
    assert str(trigger) == "cron[day='6,8']"
    start_date = localize(datetime(2009, 12, 31), timezone)
    correct_next_date = localize(datetime(2010, 1, 6), timezone)
    assert trigger.get_next_fire_time(None, start_date) == correct_next_date


def test_cron_faulty_expr(timezone):
    pytest.raises(ValueError, CronTrigger, year="2009-fault", timezone=timezone)


def test_cron_increment_weekday(timezone):
    """
    Tests that incrementing the weekday field in the process of calculating the next matching
    date won't cause problems.

    """
    trigger = CronTrigger(hour="5-6", timezone=timezone)
    assert repr(trigger) == "<CronTrigger (hour='5-6', timezone='Europe/Berlin')>"
    assert str(trigger) == "cron[hour='5-6']"
    start_date = localize(datetime(2009, 9, 25, 7), timezone)
    correct_next_date = localize(datetime(2009, 9, 26, 5), timezone)
    assert trigger.get_next_fire_time(None, start_date) == correct_next_date


def test_cron_bad_kwarg(timezone):
    pytest.raises(TypeError, CronTrigger, second=0, third=1, timezone=timezone)


def test_month_rollover(timezone):
    trigger = CronTrigger(timezone=timezone, day=30)
    now = localize(datetime(2016, 2, 1), timezone)
    expected = localize(datetime(2016, 3, 30), timezone)
    assert trigger.get_next_fire_time(None, now) == expected


def test_timezone_from_start_date(timezone):
    """
    Tests that the trigger takes the timezone from the start_date parameter if no
    timezone is supplied.

    """
    start_date = localize(datetime(2014, 4, 13, 5, 30), timezone)
    trigger = CronTrigger(year=2014, hour=4, start_date=start_date)
    assert trigger.timezone.key == "Europe/Berlin"


def test_end_date(timezone):
    end_date = localize(datetime(2014, 4, 13, 3), timezone)
    trigger = CronTrigger(year=2014, hour=4, end_date=end_date)

    start_date = localize(datetime(2014, 4, 13, 2, 30), timezone)
    assert trigger.get_next_fire_time(
        None, start_date - timedelta(1)
    ) == start_date.replace(day=12, hour=4, minute=0)
    assert trigger.get_next_fire_time(None, start_date) is None


def test_different_tz(timezone):
    alter_tz = pytz.FixedOffset(-600)
    trigger = CronTrigger(year=2009, week=15, day_of_week=2, timezone=timezone)
    assert repr(trigger) == (
        "<CronTrigger (year='2009', week='15', day_of_week='2', "
        "timezone='Europe/Berlin')>"
    )
    assert str(trigger) == "cron[year='2009', week='15', day_of_week='2']"
    start_date = alter_tz.localize(datetime(2008, 12, 31, 22))
    correct_next_date = localize(datetime(2009, 4, 8), timezone)
    assert trigger.get_next_fire_time(None, start_date) == correct_next_date


@pytest.mark.parametrize(
    "trigger_args, start_date, start_date_fold, correct_next_date",
    [
        ({"hour": 8}, datetime(2013, 3, 9, 12), False, datetime(2013, 3, 10, 8)),
        ({"hour": 8}, datetime(2013, 11, 2, 12), True, datetime(2013, 11, 3, 8)),
        (
            {"minute": "*/30"},
            datetime(2013, 3, 10, 1, 35),
            1,
            datetime(2013, 3, 10, 3),
        ),
        (
            {"minute": "*/30"},
            datetime(2013, 11, 3, 1, 35),
            0,
            datetime(2013, 11, 3, 1),
        ),
    ],
    ids=[
        "absolute_spring",
        "absolute_autumn",
        "interval_spring",
        "interval_autumn",
    ],
)
def test_dst_change(trigger_args, start_date, start_date_fold, correct_next_date):
    """
    Making sure that CronTrigger works correctly when crossing the DST switch threshold.
    Note that you should explicitly compare datetimes as strings to avoid the internal datetime
    comparison which would test for equality in the UTC timezone.

    """
    timezone = ZoneInfo("US/Eastern")
    trigger = CronTrigger(timezone=timezone, **trigger_args)
    start_date = start_date.replace(tzinfo=timezone, fold=start_date_fold)
    correct_next_date = correct_next_date.replace(tzinfo=timezone, fold=1)
    assert str(trigger.get_next_fire_time(None, start_date)) == str(correct_next_date)


def test_timezone_change(timezone):
    """
    Ensure that get_next_fire_time method returns datetimes in the timezone of the trigger and
    not in the timezone of the passed in start_date.

    """
    est = pytz.FixedOffset(-300)
    cst = pytz.FixedOffset(-360)
    trigger = CronTrigger(hour=11, minute="*/5", timezone=est)
    start_date = cst.localize(datetime(2009, 9, 26, 10, 16))
    correct_next_date = est.localize(datetime(2009, 9, 26, 11, 20))
    assert str(trigger.get_next_fire_time(None, start_date)) == str(correct_next_date)


def test_pickle(timezone):
    """Test that the trigger is pickleable."""

    trigger = CronTrigger(
        year=2016,
        month="5-6",
        day="20-28",
        hour=7,
        minute=25,
        second="*",
        timezone=timezone,
    )
    data = pickle.dumps(trigger, 2)
    trigger2 = pickle.loads(data)

    for attr in CronTrigger.__slots__:
        assert getattr(trigger2, attr) == getattr(trigger, attr)


def test_jitter_produces_differrent_valid_results(timezone):
    trigger = CronTrigger(minute="*", jitter=5)
    now = localize(datetime(2017, 11, 12, 6, 55, 30), timezone)

    results = set()
    for _ in range(0, 100):
        next_fire_time = trigger.get_next_fire_time(None, now)
        results.add(next_fire_time)
        assert timedelta(seconds=25) <= (next_fire_time - now) <= timedelta(seconds=35)

    assert 1 < len(results)


def test_jitter_with_timezone(timezone):
    est = pytz.FixedOffset(-300)
    cst = pytz.FixedOffset(-360)
    trigger = CronTrigger(hour=11, minute="*/5", timezone=est, jitter=5)
    start_date = cst.localize(datetime(2009, 9, 26, 10, 16))
    correct_next_date = est.localize(datetime(2009, 9, 26, 11, 20))
    for _ in range(0, 100):
        assert abs(
            trigger.get_next_fire_time(None, start_date) - correct_next_date
        ) <= timedelta(seconds=5)


@pytest.mark.parametrize(
    "trigger_args, start_date, fold, correct_next_date",
    [
        ({"hour": 8}, datetime(2013, 3, 9, 12), False, datetime(2013, 3, 10, 8)),
        ({"hour": 8}, datetime(2013, 11, 2, 12), True, datetime(2013, 11, 3, 8)),
        (
            {"minute": "*/30"},
            datetime(2013, 3, 10, 1, 35),
            False,
            datetime(2013, 3, 10, 3),
        ),
        (
            {"minute": "*/30"},
            datetime(2013, 11, 3, 1, 35),
            True,
            datetime(2013, 11, 3, 1),
        ),
    ],
    ids=[
        "absolute_spring",
        "absolute_autumn",
        "interval_spring",
        "interval_autumn",
    ],
)
def test_jitter_dst_change(trigger_args, start_date, fold, correct_next_date):
    timezone = ZoneInfo("US/Eastern")
    trigger = CronTrigger(timezone=timezone, jitter=5, **trigger_args)
    start_date = start_date.replace(tzinfo=timezone)
    correct_next_date = correct_next_date.replace(tzinfo=timezone)

    for _ in range(0, 100):
        next_fire_time = trigger.get_next_fire_time(None, start_date)
        assert abs(next_fire_time - correct_next_date) <= timedelta(seconds=5)


def test_jitter_with_end_date(timezone):
    now = localize(datetime(2017, 11, 12, 6, 55, 30), timezone)
    end_date = localize(datetime(2017, 11, 12, 6, 56, 0), timezone)
    trigger = CronTrigger(minute="*", jitter=5, end_date=end_date)

    for _ in range(0, 100):
        next_fire_time = trigger.get_next_fire_time(None, now)
        assert next_fire_time is None or next_fire_time <= end_date


@pytest.mark.parametrize(
    "values, expected",
    [
        (
            dict(day="*/31"),
            r"Error validating expression '\*/31': the step value \(31\) is higher "
            r"than the total range of the expression \(30\)",
        ),
        (
            dict(day="4-6/3"),
            r"Error validating expression '4-6/3': the step value \(3\) is higher "
            r"than the total range of the expression \(2\)",
        ),
        (
            dict(hour="0-24"),
            r"Error validating expression '0-24': the last value \(24\) is higher "
            r"than the maximum value \(23\)",
        ),
        (
            dict(day="0-3"),
            r"Error validating expression '0-3': the first value \(0\) is lower "
            r"than the minimum value \(1\)",
        ),
    ],
    ids=[
        "too_large_step_all",
        "too_large_step_range",
        "too_high_last",
        "too_low_first",
    ],
)
def test_invalid_ranges(values, expected):
    pytest.raises(ValueError, CronTrigger, **values).match(expected)


@pytest.mark.parametrize(
    "expr, expected_repr",
    [
        (
            "* * * * *",
            "<CronTrigger (month='*', day='*', day_of_week='*', hour='*', minute='*', "
            "timezone='Europe/Berlin')>",
        ),
        (
            "0-14 * 14-28 jul fri",
            "<CronTrigger (month='jul', day='14-28', day_of_week='fri', hour='*', minute='0-14', "
            "timezone='Europe/Berlin')>",
        ),
        (
            " 0-14   * 14-28   jul       fri",
            "<CronTrigger (month='jul', day='14-28', day_of_week='fri', hour='*', minute='0-14', "
            "timezone='Europe/Berlin')>",
        ),
    ],
    ids=["always", "assorted", "multiple_spaces_in_format"],
)
def test_from_crontab(expr, expected_repr, timezone):
    trigger = CronTrigger.from_crontab(expr, timezone)
    assert repr(trigger) == expected_repr
