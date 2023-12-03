from __future__ import annotations

import sys
from datetime import datetime

import pytest

from apscheduler.triggers.cron import CronTrigger

if sys.version_info >= (3, 9):
    from zoneinfo import ZoneInfo
else:
    from backports.zoneinfo import ZoneInfo


def test_invalid_expression():
    exc = pytest.raises(ValueError, CronTrigger, year="2009-fault")
    exc.match("Unrecognized expression '2009-fault' for field 'year'")


def test_invalid_step():
    exc = pytest.raises(ValueError, CronTrigger, year="2009/0")
    exc.match("step must be positive, got: 0")


def test_invalid_range():
    exc = pytest.raises(ValueError, CronTrigger, year="2009-2008")
    exc.match("The minimum value in a range must not be higher than the maximum")


@pytest.mark.parametrize("expr", ["fab", "jan-fab"], ids=["start", "end"])
def test_invalid_month_name(expr):
    exc = pytest.raises(ValueError, CronTrigger, month=expr)
    exc.match("Invalid month name 'fab'")


@pytest.mark.parametrize("expr", ["web", "mon-web"], ids=["start", "end"])
def test_invalid_weekday_name(expr):
    exc = pytest.raises(ValueError, CronTrigger, day_of_week=expr)
    exc.match("Invalid weekday name 'web'")


def test_invalid_weekday_position_name():
    exc = pytest.raises(ValueError, CronTrigger, day="1st web")
    exc.match("Invalid weekday name 'web'")


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


def test_cron_trigger_1(timezone, serializer):
    start_time = datetime(2008, 12, 1, tzinfo=timezone)
    trigger = CronTrigger(
        year="2009/2",
        month="1-4/3",
        day="5-6",
        start_time=start_time,
        timezone=timezone,
    )

    # since `next` is modifying the trigger, we call it before serializing
    # to make sure the serialization works correctly also for modified triggers
    assert trigger.next() == datetime(2009, 1, 5, tzinfo=timezone)

    if serializer:
        trigger = serializer.deserialize(serializer.serialize(trigger))

    assert trigger.next() == datetime(2009, 1, 6, tzinfo=timezone)
    assert trigger.next() == datetime(2009, 4, 5, tzinfo=timezone)
    assert trigger.next() == datetime(2009, 4, 6, tzinfo=timezone)
    assert trigger.next() == datetime(2011, 1, 5, tzinfo=timezone)
    assert repr(trigger) == (
        "CronTrigger(year='2009/2', month='1-4/3', day='5-6', week='*', "
        "day_of_week='*', hour='0', minute='0', second='0', "
        "start_time='2008-12-01T00:00:00+01:00', timezone='Europe/Berlin')"
    )


def test_cron_trigger_2(timezone, serializer):
    start_time = datetime(2009, 10, 14, tzinfo=timezone)
    trigger = CronTrigger(
        year="2009/2", month="1-3", day="5", start_time=start_time, timezone=timezone
    )
    if serializer:
        trigger = serializer.deserialize(serializer.serialize(trigger))

    assert trigger.next() == datetime(2011, 1, 5, tzinfo=timezone)
    assert trigger.next() == datetime(2011, 2, 5, tzinfo=timezone)
    assert trigger.next() == datetime(2011, 3, 5, tzinfo=timezone)
    assert trigger.next() == datetime(2013, 1, 5, tzinfo=timezone)
    assert repr(trigger) == (
        "CronTrigger(year='2009/2', month='1-3', day='5', week='*', "
        "day_of_week='*', hour='0', minute='0', second='0', "
        "start_time='2009-10-14T00:00:00+02:00', timezone='Europe/Berlin')"
    )


def test_cron_trigger_3(timezone, serializer):
    start_time = datetime(2009, 1, 1, tzinfo=timezone)
    trigger = CronTrigger(
        year="2009",
        month="feb-dec",
        hour="8-9",
        start_time=start_time,
        timezone=timezone,
    )
    if serializer:
        trigger = serializer.deserialize(serializer.serialize(trigger))

    assert trigger.next() == datetime(2009, 2, 1, 8, tzinfo=timezone)
    assert trigger.next() == datetime(2009, 2, 1, 9, tzinfo=timezone)
    assert trigger.next() == datetime(2009, 2, 2, 8, tzinfo=timezone)
    assert repr(trigger) == (
        "CronTrigger(year='2009', month='feb-dec', day='*', week='*', "
        "day_of_week='*', hour='8-9', minute='0', second='0', "
        "start_time='2009-01-01T00:00:00+01:00', timezone='Europe/Berlin')"
    )


def test_cron_trigger_4(timezone, serializer):
    start_time = datetime(2012, 2, 1, tzinfo=timezone)
    trigger = CronTrigger(
        year="2012", month="2", day="last", start_time=start_time, timezone=timezone
    )
    if serializer:
        trigger = serializer.deserialize(serializer.serialize(trigger))

    assert trigger.next() == datetime(2012, 2, 29, tzinfo=timezone)
    assert repr(trigger) == (
        "CronTrigger(year='2012', month='2', day='last', week='*', "
        "day_of_week='*', hour='0', minute='0', second='0', "
        "start_time='2012-02-01T00:00:00+01:00', timezone='Europe/Berlin')"
    )


@pytest.mark.parametrize("expr", ["3-5", "wed-fri"], ids=["numeric", "text"])
def test_weekday_overlap(timezone, serializer, expr):
    start_time = datetime(2009, 1, 1, tzinfo=timezone)
    trigger = CronTrigger(
        year=2009,
        month=1,
        day="6-10",
        day_of_week=expr,
        start_time=start_time,
        timezone=timezone,
    )
    if serializer:
        trigger = serializer.deserialize(serializer.serialize(trigger))

    assert trigger.next() == datetime(2009, 1, 7, tzinfo=timezone)
    assert repr(trigger) == (
        "CronTrigger(year='2009', month='1', day='6-10', week='*', "
        "day_of_week='wed-fri', hour='0', minute='0', second='0', "
        "start_time='2009-01-01T00:00:00+01:00', timezone='Europe/Berlin')"
    )


def test_weekday_range(timezone, serializer):
    start_time = datetime(2020, 1, 1, tzinfo=timezone)
    trigger = CronTrigger(
        year=2020,
        month=1,
        week=1,
        day_of_week="fri-sun",
        start_time=start_time,
        timezone=timezone,
    )
    if serializer:
        trigger = serializer.deserialize(serializer.serialize(trigger))

    assert trigger.next() == datetime(2020, 1, 3, tzinfo=timezone)
    assert trigger.next() == datetime(2020, 1, 4, tzinfo=timezone)
    assert trigger.next() == datetime(2020, 1, 5, tzinfo=timezone)
    assert trigger.next() is None
    assert repr(trigger) == (
        "CronTrigger(year='2020', month='1', day='*', week='1', "
        "day_of_week='fri-sun', hour='0', minute='0', second='0', "
        "start_time='2020-01-01T00:00:00+01:00', timezone='Europe/Berlin')"
    )


def test_last_weekday(timezone, serializer):
    start_time = datetime(2020, 1, 1, tzinfo=timezone)
    trigger = CronTrigger(
        year=2020, day="last sun", start_time=start_time, timezone=timezone
    )
    if serializer:
        trigger = serializer.deserialize(serializer.serialize(trigger))

    assert trigger.next() == datetime(2020, 1, 26, tzinfo=timezone)
    assert trigger.next() == datetime(2020, 2, 23, tzinfo=timezone)
    assert trigger.next() == datetime(2020, 3, 29, tzinfo=timezone)
    assert repr(trigger) == (
        "CronTrigger(year='2020', month='*', day='last sun', week='*', "
        "day_of_week='*', hour='0', minute='0', second='0', "
        "start_time='2020-01-01T00:00:00+01:00', timezone='Europe/Berlin')"
    )


def test_increment_weekday(timezone, serializer):
    """
    Tests that incrementing the weekday field in the process of calculating the next
    matching date won't cause problems.

    """
    start_time = datetime(2009, 9, 25, 7, tzinfo=timezone)
    trigger = CronTrigger(hour="5-6", start_time=start_time, timezone=timezone)
    if serializer:
        trigger = serializer.deserialize(serializer.serialize(trigger))

    assert trigger.next() == datetime(2009, 9, 26, 5, tzinfo=timezone)
    assert repr(trigger) == (
        "CronTrigger(year='*', month='*', day='*', week='*', "
        "day_of_week='*', hour='5-6', minute='0', second='0', "
        "start_time='2009-09-25T07:00:00+02:00', timezone='Europe/Berlin')"
    )


def test_month_rollover(timezone, serializer):
    start_time = datetime(2016, 2, 1, tzinfo=timezone)
    trigger = CronTrigger(day=30, start_time=start_time, timezone=timezone)
    if serializer:
        trigger = serializer.deserialize(serializer.serialize(trigger))

    assert trigger.next() == datetime(2016, 3, 30, tzinfo=timezone)
    assert trigger.next() == datetime(2016, 4, 30, tzinfo=timezone)


@pytest.mark.parametrize("weekday", ["1,0", "mon,sun"], ids=["numeric", "text"])
def test_weekday_nomatch(timezone, serializer, weekday):
    start_time = datetime(2009, 1, 1, tzinfo=timezone)
    trigger = CronTrigger(
        year=2009,
        month=1,
        day="6-10",
        day_of_week=weekday,
        start_time=start_time,
        timezone=timezone,
    )
    if serializer:
        trigger = serializer.deserialize(serializer.serialize(trigger))

    assert trigger.next() is None
    assert repr(trigger) == (
        "CronTrigger(year='2009', month='1', day='6-10', week='*', "
        "day_of_week='mon,sun', hour='0', minute='0', second='0', "
        "start_time='2009-01-01T00:00:00+01:00', timezone='Europe/Berlin')"
    )


def test_weekday_positional(timezone, serializer):
    start_time = datetime(2009, 1, 1, tzinfo=timezone)
    trigger = CronTrigger(
        year=2009, month=1, day="4th wed", start_time=start_time, timezone=timezone
    )
    if serializer:
        trigger = serializer.deserialize(serializer.serialize(trigger))

    assert trigger.next() == datetime(2009, 1, 28, tzinfo=timezone)
    assert repr(trigger) == (
        "CronTrigger(year='2009', month='1', day='4th wed', week='*', "
        "day_of_week='*', hour='0', minute='0', second='0', "
        "start_time='2009-01-01T00:00:00+01:00', timezone='Europe/Berlin')"
    )


def test_end_time(timezone, serializer):
    """Test that next() won't produce"""
    start_time = datetime(2014, 4, 13, 2, tzinfo=timezone)
    end_time = datetime(2014, 4, 13, 4, tzinfo=timezone)
    trigger = CronTrigger(
        hour=4, start_time=start_time, end_time=end_time, timezone=timezone
    )
    if serializer:
        trigger = serializer.deserialize(serializer.serialize(trigger))

    assert trigger.next() == datetime(2014, 4, 13, 4, tzinfo=timezone)
    assert trigger.next() is None
    assert repr(trigger) == (
        "CronTrigger(year='*', month='*', day='*', week='*', "
        "day_of_week='*', hour='4', minute='0', second='0', "
        "start_time='2014-04-13T02:00:00+02:00', "
        "end_time='2014-04-13T04:00:00+02:00', timezone='Europe/Berlin')"
    )


def test_week_1(timezone, serializer):
    start_time = datetime(2009, 1, 1, tzinfo=timezone)
    trigger = CronTrigger(
        year=2009, month=2, week=8, start_time=start_time, timezone=timezone
    )
    if serializer:
        trigger = serializer.deserialize(serializer.serialize(trigger))

    for day in range(16, 23):
        assert trigger.next() == datetime(2009, 2, day, tzinfo=timezone)

    assert trigger.next() is None
    assert repr(trigger) == (
        "CronTrigger(year='2009', month='2', day='*', week='8', "
        "day_of_week='*', hour='0', minute='0', second='0', "
        "start_time='2009-01-01T00:00:00+01:00', timezone='Europe/Berlin')"
    )


@pytest.mark.parametrize("weekday", [3, "wed"], ids=["numeric", "text"])
def test_week_2(timezone, serializer, weekday):
    start_time = datetime(2009, 1, 1, tzinfo=timezone)
    trigger = CronTrigger(
        year=2009,
        week=15,
        day_of_week=weekday,
        start_time=start_time,
        timezone=timezone,
    )
    if serializer:
        trigger = serializer.deserialize(serializer.serialize(trigger))

    assert trigger.next() == datetime(2009, 4, 8, tzinfo=timezone)
    assert trigger.next() is None
    assert repr(trigger) == (
        "CronTrigger(year='2009', month='*', day='*', week='15', "
        "day_of_week='wed', hour='0', minute='0', second='0', "
        "start_time='2009-01-01T00:00:00+01:00', timezone='Europe/Berlin')"
    )


@pytest.mark.parametrize(
    "trigger_args, start_time, start_time_fold, correct_next_date,"
    "correct_next_date_fold",
    [
        ({"hour": 8}, datetime(2013, 3, 9, 12), 0, datetime(2013, 3, 10, 8), 0),
        ({"hour": 8}, datetime(2013, 11, 2, 12), 0, datetime(2013, 11, 3, 8), 0),
        (
            {"minute": "*/30"},
            datetime(2013, 3, 10, 1, 35),
            0,
            datetime(2013, 3, 10, 3),
            0,
        ),
        (
            {"minute": "*/30"},
            datetime(2013, 11, 3, 1, 35),
            0,
            datetime(2013, 11, 3, 1),
            1,
        ),
    ],
    ids=["absolute_spring", "absolute_autumn", "interval_spring", "interval_autumn"],
)
def test_dst_change(
    trigger_args,
    start_time,
    start_time_fold,
    correct_next_date,
    correct_next_date_fold,
    serializer,
):
    """
    Making sure that CronTrigger works correctly when crossing the DST switch threshold.
    Note that you should explicitly compare datetimes as strings to avoid the internal
    datetime comparison which would test for equality in the UTC timezone.

    """
    timezone = ZoneInfo("US/Eastern")
    start_time = start_time.replace(tzinfo=timezone, fold=start_time_fold)
    trigger = CronTrigger(timezone=timezone, start_time=start_time, **trigger_args)
    if serializer:
        trigger = serializer.deserialize(serializer.serialize(trigger))

    assert trigger.next() == correct_next_date.replace(
        tzinfo=timezone, fold=correct_next_date_fold
    )


def test_zero_value(timezone):
    start_time = datetime(2020, 1, 1, tzinfo=timezone)
    trigger = CronTrigger(
        year=2009, month=2, hour=0, start_time=start_time, timezone=timezone
    )
    assert repr(trigger) == (
        "CronTrigger(year='2009', month='2', day='*', week='*', "
        "day_of_week='*', hour='0', minute='0', second='0', "
        "start_time='2020-01-01T00:00:00+01:00', timezone='Europe/Berlin')"
    )


def test_year_list(timezone, serializer):
    start_time = datetime(2009, 1, 1, tzinfo=timezone)
    trigger = CronTrigger(year="2009,2008", start_time=start_time, timezone=timezone)
    assert (
        repr(trigger) == "CronTrigger(year='2009,2008', month='1', day='1', week='*', "
        "day_of_week='*', hour='0', minute='0', second='0', "
        "start_time='2009-01-01T00:00:00+01:00', timezone='Europe/Berlin')"
    )
    assert trigger.next() == datetime(2009, 1, 1, tzinfo=timezone)
    assert trigger.next() is None


@pytest.mark.parametrize(
    "expr, expected_repr",
    [
        (
            "* * * * *",
            "CronTrigger(year='*', month='*', day='*', week='*', day_of_week='*', "
            "hour='*', minute='*', second='0', start_time='2020-05-19T19:53:22+02:00', "
            "timezone='Europe/Berlin')",
        ),
        (
            "0-14 * 14-28 jul fri",
            "CronTrigger(year='*', month='jul', day='14-28', week='*', "
            "day_of_week='fri', hour='*', minute='0-14', second='0', "
            "start_time='2020-05-19T19:53:22+02:00', timezone='Europe/Berlin')",
        ),
        (
            " 0-14   * 14-28   jul       fri",
            "CronTrigger(year='*', month='jul', day='14-28', week='*', "
            "day_of_week='fri', hour='*', minute='0-14', second='0', "
            "start_time='2020-05-19T19:53:22+02:00', timezone='Europe/Berlin')",
        ),
        (
            "* * * * 1-5",
            "CronTrigger(year='*', month='*', day='*', week='*', "
            "day_of_week='mon-fri', hour='*', minute='*', second='0', "
            "start_time='2020-05-19T19:53:22+02:00', timezone='Europe/Berlin')",
        ),
        (
            "* * * * 0-3",
            "CronTrigger(year='*', month='*', day='*', week='*', "
            "day_of_week='mon-wed,sun', hour='*', minute='*', second='0', "
            "start_time='2020-05-19T19:53:22+02:00', timezone='Europe/Berlin')",
        ),
        (
            "* * * * 6-1",
            "CronTrigger(year='*', month='*', day='*', week='*', "
            "day_of_week='mon,sat-sun', hour='*', minute='*', second='0', "
            "start_time='2020-05-19T19:53:22+02:00', timezone='Europe/Berlin')",
        ),
        (
            "* * * * 6-7",
            "CronTrigger(year='*', month='*', day='*', week='*', "
            "day_of_week='sat-sun', hour='*', minute='*', second='0', "
            "start_time='2020-05-19T19:53:22+02:00', timezone='Europe/Berlin')",
        ),
    ],
    ids=[
        "always",
        "assorted",
        "multiple_spaces_in_format",
        "working_week",
        "sunday_first",
        "saturday_first",
        "weekend",
    ],
)
def test_from_crontab(expr, expected_repr, timezone, serializer):
    trigger = CronTrigger.from_crontab(expr, timezone)
    trigger.start_time = datetime(2020, 5, 19, 19, 53, 22, tzinfo=timezone)
    if serializer:
        trigger = serializer.deserialize(serializer.serialize(trigger))

    assert repr(trigger) == expected_repr


def test_from_crontab_wrong_number_of_fields():
    exc = pytest.raises(ValueError, CronTrigger.from_crontab, "*")
    exc.match("Wrong number of fields; got 1, expected 5")
