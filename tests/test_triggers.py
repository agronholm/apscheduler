import pickle
import random
import sys
from datetime import datetime, timedelta, date

import pytest
import pytz

from apscheduler.triggers.base import BaseTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.combining import AndTrigger, OrTrigger, BaseCombiningTrigger

try:
    from unittest.mock import Mock
except ImportError:
    from mock import Mock


class _DummyTriggerWithJitter(BaseTrigger):
    def __init__(self, dt, jitter):
        self.dt = dt
        self.jitter = jitter

    def get_next_fire_time(self, previous_fire_time, now):
        return self._apply_jitter(self.dt, self.jitter, now)


class TestJitter(object):
    def test_jitter_disabled(self):
        dt = datetime(2017, 5, 25, 14, 49, 50)
        trigger = _DummyTriggerWithJitter(dt, None)

        now = datetime(2017, 5, 25, 13, 40, 44)
        assert trigger.get_next_fire_time(None, now) == dt

    def test_jitter_with_none_next_fire_time(self):
        trigger = _DummyTriggerWithJitter(None, 5)
        now = datetime(2017, 5, 25, 13, 40, 44)
        assert trigger.get_next_fire_time(None, now) is None

    def test_jitter_positive(self, monkeypatch):
        monkeypatch.setattr(random, 'uniform', lambda a, b: 30.)

        now = datetime(2017, 5, 25, 13, 40, 44)
        dt = datetime(2017, 5, 25, 14, 49, 50)
        expected_dt = datetime(2017, 5, 25, 14, 50, 20)

        trigger = _DummyTriggerWithJitter(dt, 60)
        assert trigger.get_next_fire_time(None, now) == expected_dt

    def test_jitter_in_future_but_initial_date_in_past(self, monkeypatch):
        monkeypatch.setattr(random, 'uniform', lambda a, b: 30.)

        now = datetime(2017, 5, 25, 13, 40, 44)
        dt = datetime(2017, 5, 25, 13, 40, 30)
        expected_dt = datetime(2017, 5, 25, 13, 41, 0)

        trigger = _DummyTriggerWithJitter(dt, 60)
        assert trigger.get_next_fire_time(None, now) == expected_dt

    def test_jitter_is_now(self, monkeypatch):
        monkeypatch.setattr(random, 'uniform', lambda a, b: 4.)

        now = datetime(2017, 5, 25, 13, 40, 44)
        dt = datetime(2017, 5, 25, 13, 40, 40)
        expected_dt = now

        trigger = _DummyTriggerWithJitter(dt, 60)
        assert trigger.get_next_fire_time(None, now) == expected_dt

    def test_jitter(self):
        now = datetime(2017, 5, 25, 13, 36, 44)
        dt = datetime(2017, 5, 25, 13, 40, 45)
        min_expected_dt = datetime(2017, 5, 25, 13, 40, 40)
        max_expected_dt = datetime(2017, 5, 25, 13, 40, 50)

        trigger = _DummyTriggerWithJitter(dt, 5)
        for _ in range(0, 100):
            assert min_expected_dt <= trigger.get_next_fire_time(None, now) <= max_expected_dt


class TestCronTrigger(object):
    def test_cron_trigger_1(self, timezone):
        trigger = CronTrigger(year='2009/2', month='1/3', day='5-13', timezone=timezone)
        assert repr(trigger) == ("<CronTrigger (year='2009/2', month='1/3', day='5-13', "
                                 "timezone='Europe/Berlin')>")
        assert str(trigger) == "cron[year='2009/2', month='1/3', day='5-13']"
        start_date = timezone.localize(datetime(2008, 12, 1))
        correct_next_date = timezone.localize(datetime(2009, 1, 5))
        assert trigger.get_next_fire_time(None, start_date) == correct_next_date

    def test_cron_trigger_2(self, timezone):
        trigger = CronTrigger(year='2009/2', month='1/3', day='5-13', timezone=timezone)
        start_date = timezone.localize(datetime(2009, 10, 14))
        correct_next_date = timezone.localize(datetime(2011, 1, 5))
        assert trigger.get_next_fire_time(None, start_date) == correct_next_date

    def test_cron_trigger_3(self, timezone):
        trigger = CronTrigger(year='2009', month='feb-dec', hour='8-10', timezone=timezone)
        assert repr(trigger) == ("<CronTrigger (year='2009', month='feb-dec', hour='8-10', "
                                 "timezone='Europe/Berlin')>")
        start_date = timezone.localize(datetime(2009, 1, 1))
        correct_next_date = timezone.localize(datetime(2009, 2, 1, 8))
        assert trigger.get_next_fire_time(None, start_date) == correct_next_date

    def test_cron_trigger_4(self, timezone):
        trigger = CronTrigger(year='2012', month='2', day='last', timezone=timezone)
        assert repr(trigger) == ("<CronTrigger (year='2012', month='2', day='last', "
                                 "timezone='Europe/Berlin')>")
        start_date = timezone.localize(datetime(2012, 2, 1))
        correct_next_date = timezone.localize(datetime(2012, 2, 29))
        assert trigger.get_next_fire_time(None, start_date) == correct_next_date

    def test_start_end_times_string(self, timezone, monkeypatch):
        monkeypatch.setattr('apscheduler.triggers.cron.get_localzone', Mock(return_value=timezone))
        trigger = CronTrigger(start_date='2016-11-05 05:06:53', end_date='2017-11-05 05:11:32')
        assert trigger.start_date == timezone.localize(datetime(2016, 11, 5, 5, 6, 53))
        assert trigger.end_date == timezone.localize(datetime(2017, 11, 5, 5, 11, 32))

    def test_cron_zero_value(self, timezone):
        trigger = CronTrigger(year=2009, month=2, hour=0, timezone=timezone)
        assert repr(trigger) == ("<CronTrigger (year='2009', month='2', hour='0', "
                                 "timezone='Europe/Berlin')>")

    def test_cron_year_list(self, timezone):
        trigger = CronTrigger(year='2009,2008', timezone=timezone)
        assert repr(trigger) == "<CronTrigger (year='2009,2008', timezone='Europe/Berlin')>"
        assert str(trigger) == "cron[year='2009,2008']"
        start_date = timezone.localize(datetime(2009, 1, 1))
        correct_next_date = timezone.localize(datetime(2009, 1, 1))
        assert trigger.get_next_fire_time(None, start_date) == correct_next_date

    def test_cron_start_date(self, timezone):
        trigger = CronTrigger(year='2009', month='2', hour='8-10',
                              start_date='2009-02-03 11:00:00', timezone=timezone)
        assert repr(trigger) == ("<CronTrigger (year='2009', month='2', hour='8-10', "
                                 "start_date='2009-02-03 11:00:00 CET', "
                                 "timezone='Europe/Berlin')>")
        assert str(trigger) == "cron[year='2009', month='2', hour='8-10']"
        start_date = timezone.localize(datetime(2009, 1, 1))
        correct_next_date = timezone.localize(datetime(2009, 2, 4, 8))
        assert trigger.get_next_fire_time(None, start_date) == correct_next_date

    def test_previous_fire_time_1(self, timezone):
        """Test for previous_fire_time arg in get_next_fire_time()"""
        trigger = CronTrigger(day="*", timezone=timezone)
        previous_fire_time = timezone.localize(datetime(2015, 11, 23))
        now = timezone.localize(datetime(2015, 11, 26))
        correct_next_date = timezone.localize(datetime(2015, 11, 24))
        assert trigger.get_next_fire_time(previous_fire_time, now) == correct_next_date

    def test_previous_fire_time_2(self, timezone):
        trigger = CronTrigger(day="*", timezone=timezone)
        previous_fire_time = timezone.localize(datetime(2015, 11, 23))
        now = timezone.localize(datetime(2015, 11, 22))
        correct_next_date = timezone.localize(datetime(2015, 11, 22))
        assert trigger.get_next_fire_time(previous_fire_time, now) == correct_next_date

    def test_previous_fire_time_3(self, timezone):
        trigger = CronTrigger(day="*", timezone=timezone)
        previous_fire_time = timezone.localize(datetime(2016, 4, 25))
        now = timezone.localize(datetime(2016, 4, 25))
        correct_next_date = timezone.localize(datetime(2016, 4, 26))
        assert trigger.get_next_fire_time(previous_fire_time, now) == correct_next_date

    def test_cron_weekday_overlap(self, timezone):
        trigger = CronTrigger(year=2009, month=1, day='6-10', day_of_week='2-4', timezone=timezone)
        assert repr(trigger) == ("<CronTrigger (year='2009', month='1', day='6-10', "
                                 "day_of_week='2-4', timezone='Europe/Berlin')>")
        assert str(trigger) == "cron[year='2009', month='1', day='6-10', day_of_week='2-4']"
        start_date = timezone.localize(datetime(2009, 1, 1))
        correct_next_date = timezone.localize(datetime(2009, 1, 7))
        assert trigger.get_next_fire_time(None, start_date) == correct_next_date

    def test_cron_weekday_nomatch(self, timezone):
        trigger = CronTrigger(year=2009, month=1, day='6-10', day_of_week='0,6', timezone=timezone)
        assert repr(trigger) == ("<CronTrigger (year='2009', month='1', day='6-10', "
                                 "day_of_week='0,6', timezone='Europe/Berlin')>")
        assert str(trigger) == "cron[year='2009', month='1', day='6-10', day_of_week='0,6']"
        start_date = timezone.localize(datetime(2009, 1, 1))
        correct_next_date = None
        assert trigger.get_next_fire_time(None, start_date) == correct_next_date

    def test_cron_weekday_positional(self, timezone):
        trigger = CronTrigger(year=2009, month=1, day='4th wed', timezone=timezone)
        assert repr(trigger) == ("<CronTrigger (year='2009', month='1', day='4th wed', "
                                 "timezone='Europe/Berlin')>")
        assert str(trigger) == "cron[year='2009', month='1', day='4th wed']"
        start_date = timezone.localize(datetime(2009, 1, 1))
        correct_next_date = timezone.localize(datetime(2009, 1, 28))
        assert trigger.get_next_fire_time(None, start_date) == correct_next_date

    def test_week_1(self, timezone):
        trigger = CronTrigger(year=2009, month=2, week=8, timezone=timezone)
        assert repr(trigger) == ("<CronTrigger (year='2009', month='2', week='8', "
                                 "timezone='Europe/Berlin')>")
        assert str(trigger) == "cron[year='2009', month='2', week='8']"
        start_date = timezone.localize(datetime(2009, 1, 1))
        correct_next_date = timezone.localize(datetime(2009, 2, 16))
        assert trigger.get_next_fire_time(None, start_date) == correct_next_date

    def test_week_2(self, timezone):
        trigger = CronTrigger(year=2009, week=15, day_of_week=2, timezone=timezone)
        assert repr(trigger) == ("<CronTrigger (year='2009', week='15', day_of_week='2', "
                                 "timezone='Europe/Berlin')>")
        assert str(trigger) == "cron[year='2009', week='15', day_of_week='2']"
        start_date = timezone.localize(datetime(2009, 1, 1))
        correct_next_date = timezone.localize(datetime(2009, 4, 8))
        assert trigger.get_next_fire_time(None, start_date) == correct_next_date

    def test_cron_extra_coverage(self, timezone):
        # This test has no value other than patching holes in test coverage
        trigger = CronTrigger(day='6,8', timezone=timezone)
        assert repr(trigger) == "<CronTrigger (day='6,8', timezone='Europe/Berlin')>"
        assert str(trigger) == "cron[day='6,8']"
        start_date = timezone.localize(datetime(2009, 12, 31))
        correct_next_date = timezone.localize(datetime(2010, 1, 6))
        assert trigger.get_next_fire_time(None, start_date) == correct_next_date

    def test_cron_faulty_expr(self, timezone):
        pytest.raises(ValueError, CronTrigger, year='2009-fault', timezone=timezone)

    def test_cron_increment_weekday(self, timezone):
        """
        Tests that incrementing the weekday field in the process of calculating the next matching
        date won't cause problems.

        """
        trigger = CronTrigger(hour='5-6', timezone=timezone)
        assert repr(trigger) == "<CronTrigger (hour='5-6', timezone='Europe/Berlin')>"
        assert str(trigger) == "cron[hour='5-6']"
        start_date = timezone.localize(datetime(2009, 9, 25, 7))
        correct_next_date = timezone.localize(datetime(2009, 9, 26, 5))
        assert trigger.get_next_fire_time(None, start_date) == correct_next_date

    def test_cron_bad_kwarg(self, timezone):
        pytest.raises(TypeError, CronTrigger, second=0, third=1, timezone=timezone)

    def test_month_rollover(self, timezone):
        trigger = CronTrigger(timezone=timezone, day=30)
        now = timezone.localize(datetime(2016, 2, 1))
        expected = timezone.localize(datetime(2016, 3, 30))
        assert trigger.get_next_fire_time(None, now) == expected

    def test_timezone_from_start_date(self, timezone):
        """
        Tests that the trigger takes the timezone from the start_date parameter if no timezone is
        supplied.

        """
        start_date = timezone.localize(datetime(2014, 4, 13, 5, 30))
        trigger = CronTrigger(year=2014, hour=4, start_date=start_date)
        assert trigger.timezone == start_date.tzinfo

    def test_end_date(self, timezone):
        end_date = timezone.localize(datetime(2014, 4, 13, 3))
        trigger = CronTrigger(year=2014, hour=4, end_date=end_date)

        start_date = timezone.localize(datetime(2014, 4, 13, 2, 30))
        assert trigger.get_next_fire_time(None, start_date - timedelta(1)) == \
            start_date.replace(day=12, hour=4, minute=0)
        assert trigger.get_next_fire_time(None, start_date) is None

    def test_different_tz(self, timezone):
        alter_tz = pytz.FixedOffset(-600)
        trigger = CronTrigger(year=2009, week=15, day_of_week=2, timezone=timezone)
        assert repr(trigger) == ("<CronTrigger (year='2009', week='15', day_of_week='2', "
                                 "timezone='Europe/Berlin')>")
        assert str(trigger) == "cron[year='2009', week='15', day_of_week='2']"
        start_date = alter_tz.localize(datetime(2008, 12, 31, 22))
        correct_next_date = timezone.localize(datetime(2009, 4, 8))
        assert trigger.get_next_fire_time(None, start_date) == correct_next_date

    @pytest.mark.parametrize('trigger_args, start_date, start_date_dst, correct_next_date', [
        ({'hour': 8}, datetime(2013, 3, 9, 12), False, datetime(2013, 3, 10, 8)),
        ({'hour': 8}, datetime(2013, 11, 2, 12), True, datetime(2013, 11, 3, 8)),
        ({'minute': '*/30'}, datetime(2013, 3, 10, 1, 35), False, datetime(2013, 3, 10, 3)),
        ({'minute': '*/30'}, datetime(2013, 11, 3, 1, 35), True, datetime(2013, 11, 3, 1))
    ], ids=['absolute_spring', 'absolute_autumn', 'interval_spring', 'interval_autumn'])
    def test_dst_change(self, trigger_args, start_date, start_date_dst, correct_next_date):
        """
        Making sure that CronTrigger works correctly when crossing the DST switch threshold.
        Note that you should explicitly compare datetimes as strings to avoid the internal datetime
        comparison which would test for equality in the UTC timezone.

        """
        timezone = pytz.timezone('US/Eastern')
        trigger = CronTrigger(timezone=timezone, **trigger_args)
        start_date = timezone.localize(start_date, is_dst=start_date_dst)
        correct_next_date = timezone.localize(correct_next_date, is_dst=not start_date_dst)
        assert str(trigger.get_next_fire_time(None, start_date)) == str(correct_next_date)

    def test_timezone_change(self, timezone):
        """
        Ensure that get_next_fire_time method returns datetimes in the timezone of the trigger and
        not in the timezone of the passed in start_date.

        """
        est = pytz.FixedOffset(-300)
        cst = pytz.FixedOffset(-360)
        trigger = CronTrigger(hour=11, minute='*/5', timezone=est)
        start_date = cst.localize(datetime(2009, 9, 26, 10, 16))
        correct_next_date = est.localize(datetime(2009, 9, 26, 11, 20))
        assert str(trigger.get_next_fire_time(None, start_date)) == str(correct_next_date)

    def test_pickle(self, timezone):
        """Test that the trigger is pickleable."""

        trigger = CronTrigger(year=2016, month='5-6', day='20-28', hour=7, minute=25, second='*',
                              timezone=timezone)
        data = pickle.dumps(trigger, 2)
        trigger2 = pickle.loads(data)

        for attr in CronTrigger.__slots__:
            assert getattr(trigger2, attr) == getattr(trigger, attr)

    def test_jitter_produces_differrent_valid_results(self, timezone):
        trigger = CronTrigger(minute='*', jitter=5)
        now = timezone.localize(datetime(2017, 11, 12, 6, 55, 30))

        results = set()
        for _ in range(0, 100):
            next_fire_time = trigger.get_next_fire_time(None, now)
            results.add(next_fire_time)
            assert timedelta(seconds=25) <= (next_fire_time - now) <= timedelta(seconds=35)

        assert 1 < len(results)

    def test_jitter_with_timezone(self, timezone):
        est = pytz.FixedOffset(-300)
        cst = pytz.FixedOffset(-360)
        trigger = CronTrigger(hour=11, minute='*/5', timezone=est, jitter=5)
        start_date = cst.localize(datetime(2009, 9, 26, 10, 16))
        correct_next_date = est.localize(datetime(2009, 9, 26, 11, 20))
        for _ in range(0, 100):
            assert abs(trigger.get_next_fire_time(None, start_date) -
                       correct_next_date) <= timedelta(seconds=5)

    @pytest.mark.parametrize('trigger_args, start_date, start_date_dst, correct_next_date', [
        ({'hour': 8}, datetime(2013, 3, 9, 12), False, datetime(2013, 3, 10, 8)),
        ({'hour': 8}, datetime(2013, 11, 2, 12), True, datetime(2013, 11, 3, 8)),
        ({'minute': '*/30'}, datetime(2013, 3, 10, 1, 35), False, datetime(2013, 3, 10, 3)),
        ({'minute': '*/30'}, datetime(2013, 11, 3, 1, 35), True, datetime(2013, 11, 3, 1))
    ], ids=['absolute_spring', 'absolute_autumn', 'interval_spring', 'interval_autumn'])
    def test_jitter_dst_change(self, trigger_args, start_date, start_date_dst, correct_next_date):
        timezone = pytz.timezone('US/Eastern')
        trigger = CronTrigger(timezone=timezone, jitter=5, **trigger_args)
        start_date = timezone.localize(start_date, is_dst=start_date_dst)
        correct_next_date = timezone.localize(correct_next_date, is_dst=not start_date_dst)

        for _ in range(0, 100):
            next_fire_time = trigger.get_next_fire_time(None, start_date)
            assert abs(next_fire_time - correct_next_date) <= timedelta(seconds=5)

    def test_jitter_with_end_date(self, timezone):
        now = timezone.localize(datetime(2017, 11, 12, 6, 55, 30))
        end_date = timezone.localize(datetime(2017, 11, 12, 6, 56, 0))
        trigger = CronTrigger(minute='*', jitter=5, end_date=end_date)

        for _ in range(0, 100):
            next_fire_time = trigger.get_next_fire_time(None, now)
            assert next_fire_time is None or next_fire_time <= end_date

    @pytest.mark.parametrize('values, expected', [
        (dict(day='*/31'), r"Error validating expression '\*/31': the step value \(31\) is higher "
                           r"than the total range of the expression \(30\)"),
        (dict(day='4-6/3'), r"Error validating expression '4-6/3': the step value \(3\) is higher "
                            r"than the total range of the expression \(2\)"),
        (dict(hour='0-24'), r"Error validating expression '0-24': the last value \(24\) is higher "
                            r"than the maximum value \(23\)"),
        (dict(day='0-3'), r"Error validating expression '0-3': the first value \(0\) is lower "
                          r"than the minimum value \(1\)")
    ], ids=['too_large_step_all', 'too_large_step_range', 'too_high_last', 'too_low_first'])
    def test_invalid_ranges(self, values, expected):
        pytest.raises(ValueError, CronTrigger, **values).match(expected)

    @pytest.mark.parametrize('expr, expected_repr', [
        ('* * * * *',
         "<CronTrigger (month='*', day='*', day_of_week='*', hour='*', minute='*', "
         "timezone='Europe/Berlin')>"),
        ('0-14 * 14-28 jul fri',
         "<CronTrigger (month='jul', day='14-28', day_of_week='fri', hour='*', minute='0-14', "
         "timezone='Europe/Berlin')>"),
        (' 0-14   * 14-28   jul       fri',
         "<CronTrigger (month='jul', day='14-28', day_of_week='fri', hour='*', minute='0-14', "
         "timezone='Europe/Berlin')>")
    ], ids=['always', 'assorted', 'multiple_spaces_in_format'])
    def test_from_crontab(self, expr, expected_repr, timezone):
        trigger = CronTrigger.from_crontab(expr, timezone)
        assert repr(trigger) == expected_repr


class TestDateTrigger(object):
    @pytest.mark.parametrize('run_date,alter_tz,previous,now,expected', [
        (datetime(2009, 7, 6), None, None, datetime(2008, 5, 4), datetime(2009, 7, 6)),
        (datetime(2009, 7, 6), None, None, datetime(2009, 7, 6), datetime(2009, 7, 6)),
        (datetime(2009, 7, 6), None, None, datetime(2009, 9, 2), datetime(2009, 7, 6)),
        ('2009-7-6', None, None, datetime(2009, 9, 2), datetime(2009, 7, 6)),
        (datetime(2009, 7, 6), None, datetime(2009, 7, 6), datetime(2009, 9, 2), None),
        (datetime(2009, 7, 5, 22), pytz.FixedOffset(-60), datetime(2009, 7, 6),
         datetime(2009, 7, 6), None),
        (None, pytz.FixedOffset(-120), None, datetime(2011, 4, 3, 18, 40),
         datetime(2011, 4, 3, 18, 40))
    ], ids=['earlier', 'exact', 'later', 'as text', 'previously fired', 'alternate timezone',
            'current_time'])
    def test_get_next_fire_time(self, run_date, alter_tz, previous, now, expected, timezone,
                                freeze_time):
        trigger = DateTrigger(run_date, alter_tz or timezone)
        previous = timezone.localize(previous) if previous else None
        now = timezone.localize(now)
        expected = timezone.localize(expected) if expected else None
        assert trigger.get_next_fire_time(previous, now) == expected

    @pytest.mark.parametrize('is_dst', [True, False], ids=['daylight saving', 'standard time'])
    def test_dst_change(self, is_dst):
        """
        Test that DateTrigger works during the ambiguous "fall-back" DST period.

        Note that you should explicitly compare datetimes as strings to avoid the internal datetime
        comparison which would test for equality in the UTC timezone.

        """
        eastern = pytz.timezone('US/Eastern')
        run_date = eastern.localize(datetime(2013, 10, 3, 1, 5), is_dst=is_dst)

        fire_date = eastern.normalize(run_date + timedelta(minutes=55))
        trigger = DateTrigger(run_date=fire_date, timezone=eastern)
        assert str(trigger.get_next_fire_time(None, fire_date)) == str(fire_date)

    def test_repr(self, timezone):
        trigger = DateTrigger(datetime(2009, 7, 6), timezone)
        assert repr(trigger) == "<DateTrigger (run_date='2009-07-06 00:00:00 CEST')>"

    def test_str(self, timezone):
        trigger = DateTrigger(datetime(2009, 7, 6), timezone)
        assert str(trigger) == "date[2009-07-06 00:00:00 CEST]"

    def test_pickle(self, timezone):
        """Test that the trigger is pickleable."""
        trigger = DateTrigger(date(2016, 4, 3), timezone=timezone)
        data = pickle.dumps(trigger, 2)
        trigger2 = pickle.loads(data)
        assert trigger2.run_date == trigger.run_date


class TestIntervalTrigger(object):
    @pytest.fixture()
    def trigger(self, timezone):
        return IntervalTrigger(seconds=1, start_date=datetime(2009, 8, 4, second=2),
                               timezone=timezone)

    def test_invalid_interval(self, timezone):
        pytest.raises(TypeError, IntervalTrigger, '1-6', timezone=timezone)

    def test_start_end_times_string(self, timezone, monkeypatch):
        monkeypatch.setattr('apscheduler.triggers.interval.get_localzone',
                            Mock(return_value=timezone))
        trigger = IntervalTrigger(start_date='2016-11-05 05:06:53', end_date='2017-11-05 05:11:32')
        assert trigger.start_date == timezone.localize(datetime(2016, 11, 5, 5, 6, 53))
        assert trigger.end_date == timezone.localize(datetime(2017, 11, 5, 5, 11, 32))

    def test_before(self, trigger, timezone):
        """Tests that if "start_date" is later than "now", it will return start_date."""
        now = trigger.start_date - timedelta(seconds=2)
        assert trigger.get_next_fire_time(None, now) == trigger.start_date

    def test_within(self, trigger, timezone):
        """
        Tests that if "now" is between "start_date" and the next interval, it will return the next
        interval.

        """
        now = trigger.start_date + timedelta(microseconds=1000)
        assert trigger.get_next_fire_time(None, now) == trigger.start_date + trigger.interval

    def test_no_start_date(self, timezone):
        trigger = IntervalTrigger(seconds=2, timezone=timezone)
        now = datetime.now(timezone)
        assert (trigger.get_next_fire_time(None, now) - now) <= timedelta(seconds=2)

    def test_different_tz(self, trigger, timezone):
        alter_tz = pytz.FixedOffset(-60)
        start_date = alter_tz.localize(datetime(2009, 8, 3, 22, second=2, microsecond=1000))
        correct_next_date = timezone.localize(datetime(2009, 8, 4, 1, second=3))
        assert trigger.get_next_fire_time(None, start_date) == correct_next_date

    def test_end_date(self, timezone):
        """Tests that the interval trigger won't return any datetimes past the set end time."""
        start_date = timezone.localize(datetime(2014, 5, 26))
        trigger = IntervalTrigger(minutes=5, start_date=start_date,
                                  end_date=datetime(2014, 5, 26, 0, 7), timezone=timezone)
        assert trigger.get_next_fire_time(None, start_date + timedelta(minutes=2)) == \
            start_date.replace(minute=5)
        assert trigger.get_next_fire_time(None, start_date + timedelta(minutes=6)) is None

    def test_dst_change(self):
        """
        Making sure that IntervalTrigger works during the ambiguous "fall-back" DST period.
        Note that you should explicitly compare datetimes as strings to avoid the internal datetime
        comparison which would test for equality in the UTC timezone.

        """
        eastern = pytz.timezone('US/Eastern')
        start_date = datetime(2013, 3, 1)  # Start within EDT
        trigger = IntervalTrigger(hours=1, start_date=start_date, timezone=eastern)

        datetime_edt = eastern.localize(datetime(2013, 3, 10, 1, 5), is_dst=False)
        correct_next_date = eastern.localize(datetime(2013, 3, 10, 3), is_dst=True)
        assert str(trigger.get_next_fire_time(None, datetime_edt)) == str(correct_next_date)

        datetime_est = eastern.localize(datetime(2013, 11, 3, 1, 5), is_dst=True)
        correct_next_date = eastern.localize(datetime(2013, 11, 3, 1), is_dst=False)
        assert str(trigger.get_next_fire_time(None, datetime_est)) == str(correct_next_date)

    def test_space_in_expr(self, timezone):
        trigger = CronTrigger(day='1-2, 4-7', timezone=timezone)
        assert repr(trigger) == "<CronTrigger (day='1-2,4-7', timezone='Europe/Berlin')>"

    def test_repr(self, trigger):
        if sys.version_info[:2] < (3, 7):
            timedelta_args = '0, 1'
        else:
            timedelta_args = 'seconds=1'

        assert repr(trigger) == ("<IntervalTrigger (interval=datetime.timedelta({}), "
                                 "start_date='2009-08-04 00:00:02 CEST', "
                                 "timezone='Europe/Berlin')>".format(timedelta_args))

    def test_str(self, trigger):
        assert str(trigger) == "interval[0:00:01]"

    def test_pickle(self, timezone):
        """Test that the trigger is pickleable."""

        trigger = IntervalTrigger(weeks=2, days=6, minutes=13, seconds=2,
                                  start_date=date(2016, 4, 3), timezone=timezone,
                                  jitter=12)
        data = pickle.dumps(trigger, 2)
        trigger2 = pickle.loads(data)

        for attr in IntervalTrigger.__slots__:
            assert getattr(trigger2, attr) == getattr(trigger, attr)

    def test_jitter_produces_different_valid_results(self, timezone):
        trigger = IntervalTrigger(seconds=5, timezone=timezone, jitter=3)
        now = datetime.now(timezone)

        results = set()
        for _ in range(0, 100):
            next_fire_time = trigger.get_next_fire_time(None, now)
            results.add(next_fire_time)
            assert timedelta(seconds=2) <= (next_fire_time - now) <= timedelta(seconds=8)
        assert 1 < len(results)

    @pytest.mark.parametrize('trigger_args, start_date, start_date_dst, correct_next_date', [
        ({'hours': 1}, datetime(2013, 3, 10, 1, 35), False, datetime(2013, 3, 10, 3, 35)),
        ({'hours': 1}, datetime(2013, 11, 3, 1, 35), True, datetime(2013, 11, 3, 1, 35))
    ], ids=['interval_spring', 'interval_autumn'])
    def test_jitter_dst_change(self, trigger_args, start_date, start_date_dst, correct_next_date):
        timezone = pytz.timezone('US/Eastern')
        epsilon = timedelta(seconds=1)
        start_date = timezone.localize(start_date, is_dst=start_date_dst)
        trigger = IntervalTrigger(timezone=timezone, start_date=start_date, jitter=5,
                                  **trigger_args)
        correct_next_date = timezone.localize(correct_next_date, is_dst=not start_date_dst)

        for _ in range(0, 100):
            next_fire_time = trigger.get_next_fire_time(None, start_date + epsilon)
            assert abs(next_fire_time - correct_next_date) <= timedelta(seconds=5)

    def test_jitter_with_end_date(self, timezone):
        now = timezone.localize(datetime(2017, 11, 12, 6, 55, 58))
        end_date = timezone.localize(datetime(2017, 11, 12, 6, 56, 0))
        trigger = IntervalTrigger(seconds=5, jitter=5, end_date=end_date)

        for _ in range(0, 100):
            next_fire_time = trigger.get_next_fire_time(None, now)
            assert next_fire_time is None or next_fire_time <= end_date


class TestAndTrigger(object):
    @pytest.fixture
    def trigger(self, timezone):
        return AndTrigger([
            CronTrigger(month='5-8', day='6-15',
                        end_date=timezone.localize(datetime(2017, 8, 10))),
            CronTrigger(month='6-9', day='*/3', end_date=timezone.localize(datetime(2017, 9, 7)))
        ])

    @pytest.mark.parametrize('start_time, expected', [
        (datetime(2017, 8, 6), datetime(2017, 8, 7)),
        (datetime(2017, 8, 10, 1), None)
    ], ids=['firstmatch', 'end'])
    def test_next_fire_time(self, trigger, timezone, start_time, expected):
        expected = timezone.localize(expected) if expected else None
        assert trigger.get_next_fire_time(None, timezone.localize(start_time)) == expected

    def test_jitter(self, trigger, timezone):
        trigger.jitter = 5
        start_time = timezone.localize(datetime(2017, 8, 6))
        expected = timezone.localize(datetime(2017, 8, 7))
        for _ in range(100):
            next_fire_time = trigger.get_next_fire_time(None, start_time)
            assert abs(expected - next_fire_time) <= timedelta(seconds=5)

    @pytest.mark.parametrize('jitter', [None, 5], ids=['nojitter', 'jitter'])
    def test_repr(self, trigger, jitter):
        trigger.jitter = jitter
        jitter_part = ', jitter={}'.format(jitter) if jitter else ''
        assert repr(trigger) == (
            "<AndTrigger([<CronTrigger (month='5-8', day='6-15', "
            "end_date='2017-08-10 00:00:00 CEST', timezone='Europe/Berlin')>, <CronTrigger "
            "(month='6-9', day='*/3', end_date='2017-09-07 00:00:00 CEST', "
            "timezone='Europe/Berlin')>]{})>".format(jitter_part))

    def test_str(self, trigger):
        assert str(trigger) == "and[cron[month='5-8', day='6-15'], cron[month='6-9', day='*/3']]"

    @pytest.mark.parametrize('jitter', [None, 5], ids=['nojitter', 'jitter'])
    def test_pickle(self, trigger, jitter):
        """Test that the trigger is pickleable."""
        trigger.jitter = jitter
        data = pickle.dumps(trigger, 2)
        trigger2 = pickle.loads(data)

        for attr in BaseCombiningTrigger.__slots__:
            assert repr(getattr(trigger2, attr)) == repr(getattr(trigger, attr))


class TestOrTrigger(object):
    @pytest.fixture
    def trigger(self, timezone):
        return OrTrigger([
            CronTrigger(month='5-8', day='6-15',
                        end_date=timezone.localize(datetime(2017, 8, 10))),
            CronTrigger(month='6-9', day='*/3', end_date=timezone.localize(datetime(2017, 9, 7)))
        ])

    @pytest.mark.parametrize('start_time, expected', [
        (datetime(2017, 8, 6), datetime(2017, 8, 6)),
        (datetime(2017, 9, 7, 1), None)
    ], ids=['earliest', 'end'])
    def test_next_fire_time(self, trigger, timezone, start_time, expected):
        expected = timezone.localize(expected) if expected else None
        assert trigger.get_next_fire_time(None, timezone.localize(start_time)) == expected

    def test_jitter(self, trigger, timezone):
        trigger.jitter = 5
        start_time = expected = timezone.localize(datetime(2017, 8, 6))
        for _ in range(100):
            next_fire_time = trigger.get_next_fire_time(None, start_time)
            assert abs(expected - next_fire_time) <= timedelta(seconds=5)

    @pytest.mark.parametrize('jitter', [None, 5], ids=['nojitter', 'jitter'])
    def test_repr(self, trigger, jitter):
        trigger.jitter = jitter
        jitter_part = ', jitter={}'.format(jitter) if jitter else ''
        assert repr(trigger) == (
            "<OrTrigger([<CronTrigger (month='5-8', day='6-15', "
            "end_date='2017-08-10 00:00:00 CEST', timezone='Europe/Berlin')>, <CronTrigger "
            "(month='6-9', day='*/3', end_date='2017-09-07 00:00:00 CEST', "
            "timezone='Europe/Berlin')>]{})>".format(jitter_part))

    def test_str(self, trigger):
        assert str(trigger) == "or[cron[month='5-8', day='6-15'], cron[month='6-9', day='*/3']]"

    @pytest.mark.parametrize('jitter', [None, 5], ids=['nojitter', 'jitter'])
    def test_pickle(self, trigger, jitter):
        """Test that the trigger is pickleable."""
        trigger.jitter = jitter
        data = pickle.dumps(trigger, 2)
        trigger2 = pickle.loads(data)

        for attr in BaseCombiningTrigger.__slots__:
            assert repr(getattr(trigger2, attr)) == repr(getattr(trigger, attr))
