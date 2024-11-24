import pickle
from datetime import date, datetime, timedelta

import pytest
import pytz

from apscheduler.triggers.date import DateTrigger
from apscheduler.util import localize


@pytest.mark.parametrize(
    "run_date,alter_tz,previous,now,expected",
    [
        (
            datetime(2009, 7, 6),
            None,
            None,
            datetime(2008, 5, 4),
            datetime(2009, 7, 6),
        ),
        (
            datetime(2009, 7, 6),
            None,
            None,
            datetime(2009, 7, 6),
            datetime(2009, 7, 6),
        ),
        (
            datetime(2009, 7, 6),
            None,
            None,
            datetime(2009, 9, 2),
            datetime(2009, 7, 6),
        ),
        ("2009-7-6", None, None, datetime(2009, 9, 2), datetime(2009, 7, 6)),
        (
            datetime(2009, 7, 6),
            None,
            datetime(2009, 7, 6),
            datetime(2009, 9, 2),
            None,
        ),
        (
            datetime(2009, 7, 5, 22),
            pytz.FixedOffset(-60),
            datetime(2009, 7, 6),
            datetime(2009, 7, 6),
            None,
        ),
        (
            None,
            pytz.FixedOffset(-120),
            None,
            datetime(2011, 4, 3, 18, 40),
            datetime(2011, 4, 3, 18, 40),
        ),
    ],
    ids=[
        "earlier",
        "exact",
        "later",
        "as text",
        "previously fired",
        "alternate timezone",
        "current_time",
    ],
)
def test_get_next_fire_time(
    run_date, alter_tz, previous, now, expected, timezone, freeze_time
):
    trigger = DateTrigger(run_date, alter_tz or timezone)
    previous = localize(previous, timezone) if previous else None
    now = localize(now, timezone)
    expected = localize(expected, timezone) if expected else None
    assert trigger.get_next_fire_time(previous, now) == expected


@pytest.mark.parametrize(
    "is_dst", [True, False], ids=["daylight saving", "standard time"]
)
def test_dst_change(is_dst):
    """
    Test that DateTrigger works during the ambiguous "fall-back" DST period.

    Note that you should explicitly compare datetimes as strings to avoid the internal datetime
    comparison which would test for equality in the UTC timezone.

    """
    eastern = pytz.timezone("US/Eastern")
    run_date = eastern.localize(datetime(2013, 10, 3, 1, 5), is_dst=is_dst)

    fire_date = eastern.normalize(run_date + timedelta(minutes=55))
    trigger = DateTrigger(run_date=fire_date, timezone=eastern)
    assert str(trigger.get_next_fire_time(None, fire_date)) == str(fire_date)


def test_repr(timezone):
    trigger = DateTrigger(datetime(2009, 7, 6), timezone)
    assert repr(trigger) == "<DateTrigger (run_date='2009-07-06 00:00:00 CEST')>"


def test_str(timezone):
    trigger = DateTrigger(datetime(2009, 7, 6), timezone)
    assert str(trigger) == "date[2009-07-06 00:00:00 CEST]"


def test_pickle(timezone):
    """Test that the trigger is pickleable."""
    trigger = DateTrigger(date(2016, 4, 3), timezone=timezone)
    data = pickle.dumps(trigger, 2)
    trigger2 = pickle.loads(data)
    assert trigger2.run_date == trigger.run_date
