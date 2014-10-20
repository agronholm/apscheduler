from datetime import datetime, timedelta

import pytest
import pytz

from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger


class TestCronTrigger(object):
    def test_cron_trigger_1(self, timezone):
        trigger = CronTrigger(year='2009/2', month='1/3', day='5-13', timezone=timezone)
        assert repr(trigger) == "<CronTrigger (year='2009/2', month='1/3', day='5-13')>"
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
        trigger = CronTrigger(year='2009', month='2', hour='8-10', timezone=timezone)
        assert repr(trigger) == "<CronTrigger (year='2009', month='2', hour='8-10')>"
        start_date = timezone.localize(datetime(2009, 1, 1))
        correct_next_date = timezone.localize(datetime(2009, 2, 1, 8))
        assert trigger.get_next_fire_time(None, start_date) == correct_next_date

    def test_cron_trigger_4(self, timezone):
        trigger = CronTrigger(year='2012', month='2', day='last', timezone=timezone)
        assert repr(trigger) == "<CronTrigger (year='2012', month='2', day='last')>"
        start_date = timezone.localize(datetime(2012, 2, 1))
        correct_next_date = timezone.localize(datetime(2012, 2, 29))
        assert trigger.get_next_fire_time(None, start_date) == correct_next_date

    def test_cron_zero_value(self, timezone):
        trigger = CronTrigger(year=2009, month=2, hour=0, timezone=timezone)
        assert repr(trigger) == "<CronTrigger (year='2009', month='2', hour='0')>"

    def test_cron_year_list(self, timezone):
        trigger = CronTrigger(year='2009,2008', timezone=timezone)
        assert repr(trigger) == "<CronTrigger (year='2009,2008')>"
        assert str(trigger) == "cron[year='2009,2008']"
        start_date = timezone.localize(datetime(2009, 1, 1))
        correct_next_date = timezone.localize(datetime(2009, 1, 1))
        assert trigger.get_next_fire_time(None, start_date) == correct_next_date

    def test_cron_start_date(self, timezone):
        trigger = CronTrigger(year='2009', month='2', hour='8-10', start_date='2009-02-03 11:00:00', timezone=timezone)
        assert repr(trigger) == \
            "<CronTrigger (year='2009', month='2', hour='8-10', start_date='2009-02-03 11:00:00 CET')>"
        assert str(trigger) == "cron[year='2009', month='2', hour='8-10']"
        start_date = timezone.localize(datetime(2009, 1, 1))
        correct_next_date = timezone.localize(datetime(2009, 2, 4, 8))
        assert trigger.get_next_fire_time(None, start_date) == correct_next_date

    def test_cron_weekday_overlap(self, timezone):
        trigger = CronTrigger(year=2009, month=1, day='6-10', day_of_week='2-4', timezone=timezone)
        assert repr(trigger) == "<CronTrigger (year='2009', month='1', day='6-10', day_of_week='2-4')>"
        assert str(trigger) == "cron[year='2009', month='1', day='6-10', day_of_week='2-4']"
        start_date = timezone.localize(datetime(2009, 1, 1))
        correct_next_date = timezone.localize(datetime(2009, 1, 7))
        assert trigger.get_next_fire_time(None, start_date) == correct_next_date

    def test_cron_weekday_nomatch(self, timezone):
        trigger = CronTrigger(year=2009, month=1, day='6-10', day_of_week='0,6', timezone=timezone)
        assert repr(trigger) == "<CronTrigger (year='2009', month='1', day='6-10', day_of_week='0,6')>"
        assert str(trigger) == "cron[year='2009', month='1', day='6-10', day_of_week='0,6']"
        start_date = timezone.localize(datetime(2009, 1, 1))
        correct_next_date = None
        assert trigger.get_next_fire_time(None, start_date) == correct_next_date

    def test_cron_weekday_positional(self, timezone):
        trigger = CronTrigger(year=2009, month=1, day='4th wed', timezone=timezone)
        assert repr(trigger) == "<CronTrigger (year='2009', month='1', day='4th wed')>"
        assert str(trigger) == "cron[year='2009', month='1', day='4th wed']"
        start_date = timezone.localize(datetime(2009, 1, 1))
        correct_next_date = timezone.localize(datetime(2009, 1, 28))
        assert trigger.get_next_fire_time(None, start_date) == correct_next_date

    def test_week_1(self, timezone):
        trigger = CronTrigger(year=2009, month=2, week=8, timezone=timezone)
        assert repr(trigger) == "<CronTrigger (year='2009', month='2', week='8')>"
        assert str(trigger) == "cron[year='2009', month='2', week='8']"
        start_date = timezone.localize(datetime(2009, 1, 1))
        correct_next_date = timezone.localize(datetime(2009, 2, 16))
        assert trigger.get_next_fire_time(None, start_date) == correct_next_date

    def test_week_2(self, timezone):
        trigger = CronTrigger(year=2009, week=15, day_of_week=2, timezone=timezone)
        assert repr(trigger) == "<CronTrigger (year='2009', week='15', day_of_week='2')>"
        assert str(trigger) == "cron[year='2009', week='15', day_of_week='2']"
        start_date = timezone.localize(datetime(2009, 1, 1))
        correct_next_date = timezone.localize(datetime(2009, 4, 8))
        assert trigger.get_next_fire_time(None, start_date) == correct_next_date

    def test_cron_extra_coverage(self, timezone):
        # This test has no value other than patching holes in test coverage
        trigger = CronTrigger(day='6,8', timezone=timezone)
        assert repr(trigger) == "<CronTrigger (day='6,8')>"
        assert str(trigger) == "cron[day='6,8']"
        start_date = timezone.localize(datetime(2009, 12, 31))
        correct_next_date = timezone.localize(datetime(2010, 1, 6))
        assert trigger.get_next_fire_time(None, start_date) == correct_next_date

    def test_cron_faulty_expr(self, timezone):
        pytest.raises(ValueError, CronTrigger, year='2009-fault', timezone=timezone)

    def test_cron_increment_weekday(self, timezone):
        """
        Tests that incrementing the weekday field in the process of calculating the next matching date won't cause
        problems.
        """

        trigger = CronTrigger(hour='5-6', timezone=timezone)
        assert repr(trigger) == "<CronTrigger (hour='5-6')>"
        assert str(trigger) == "cron[hour='5-6']"
        start_date = timezone.localize(datetime(2009, 9, 25, 7))
        correct_next_date = timezone.localize(datetime(2009, 9, 26, 5))
        assert trigger.get_next_fire_time(None, start_date) == correct_next_date

    def test_cron_bad_kwarg(self, timezone):
        pytest.raises(TypeError, CronTrigger, second=0, third=1, timezone=timezone)

    def test_timezone_from_start_date(self, timezone):
        """Tests that the trigger takes the timezone from the start_date parameter if no timezone is supplied."""

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
        assert repr(trigger) == "<CronTrigger (year='2009', week='15', day_of_week='2')>"
        assert str(trigger) == "cron[year='2009', week='15', day_of_week='2']"
        start_date = alter_tz.localize(datetime(2008, 12, 31, 22))
        correct_next_date = timezone.localize(datetime(2009, 4, 8))
        assert trigger.get_next_fire_time(None, start_date) == correct_next_date

    def test_dst_change(self):
        """
        Making sure that CronTrigger works correctly when crossing the DST switch threshold.
        Note that you should explicitly compare datetimes as strings to avoid the internal datetime comparison which
        would test for equality in the UTC timezone.
        """

        eastern = pytz.timezone('US/Eastern')
        trigger = CronTrigger(minute='*/30', timezone=eastern)

        datetime_edt = eastern.localize(datetime(2013, 11, 3, 1, 5), is_dst=True)
        correct_next_date = eastern.localize(datetime(2013, 11, 3, 1, 30), is_dst=True)
        assert str(trigger.get_next_fire_time(None, datetime_edt)) == str(correct_next_date)

        datetime_edt = eastern.localize(datetime(2013, 11, 3, 1, 35), is_dst=True)
        correct_next_date = eastern.localize(datetime(2013, 11, 3, 1), is_dst=False)
        assert str(trigger.get_next_fire_time(None, datetime_edt)) == str(correct_next_date)

    def test_timezone_change(self, timezone):
        """
        Ensure that get_next_fire_time method returns datetimes in the timezone of the trigger and not in the timezone
        of the passed in start_date.
        """

        est = pytz.FixedOffset(-300)
        cst = pytz.FixedOffset(-360)
        trigger = CronTrigger(hour=11, minute='*/5', timezone=est)
        start_date = cst.localize(datetime(2009, 9, 26, 10, 16))
        correct_next_date = est.localize(datetime(2009, 9, 26, 11, 20))
        assert str(trigger.get_next_fire_time(None, start_date)) == str(correct_next_date)


class TestDateTrigger(object):
    @pytest.mark.parametrize('run_date,alter_tz,previous,now,expected', [
        (datetime(2009, 7, 6), None, None, datetime(2008, 5, 4), datetime(2009, 7, 6)),
        (datetime(2009, 7, 6), None, None, datetime(2009, 7, 6), datetime(2009, 7, 6)),
        (datetime(2009, 7, 6), None, None, datetime(2009, 9, 2), datetime(2009, 7, 6)),
        ('2009-7-6', None, None, datetime(2009, 9, 2), datetime(2009, 7, 6)),
        (datetime(2009, 7, 6), None, datetime(2009, 7, 6), datetime(2009, 9, 2), None),
        (datetime(2009, 7, 5, 22), pytz.FixedOffset(-60), datetime(2009, 7, 6), datetime(2009, 7, 6), None)
    ], ids=['earlier', 'exact', 'later', 'as text', 'previously fired', 'alternate timezone'])
    def test_get_next_fire_time(self, run_date, alter_tz, previous, now, expected, timezone):
        trigger = DateTrigger(run_date, alter_tz or timezone)
        previous = timezone.localize(previous) if previous else None
        now = timezone.localize(now)
        expected = timezone.localize(expected) if expected else None
        assert trigger.get_next_fire_time(previous, now) == expected

    @pytest.mark.parametrize('is_dst', [True, False], ids=['daylight saving', 'standard time'])
    def test_dst_change(self, is_dst):
        """
        Making sure that DateTrigger works during the ambiguous "fall-back" DST period.
        Note that you should explicitly compare datetimes as strings to avoid the internal datetime comparison which
        would test for equality in the UTC timezone.
        """

        eastern = pytz.timezone('US/Eastern')
        run_date = eastern.localize(datetime(2013, 11, 3, 1, 5), is_dst=is_dst)

        fire_date = eastern.normalize(run_date + timedelta(minutes=55))
        trigger = DateTrigger(run_date=fire_date, timezone=eastern)
        assert str(trigger.get_next_fire_time(None, fire_date)) == str(fire_date)

    def test_repr(self, timezone):
        trigger = DateTrigger(datetime(2009, 7, 6), timezone)
        assert repr(trigger) == "<DateTrigger (run_date='2009-07-06 00:00:00 CEST')>"

    def test_str(self, timezone):
        trigger = DateTrigger(datetime(2009, 7, 6), timezone)
        assert str(trigger) == "date[2009-07-06 00:00:00 CEST]"


class TestIntervalTrigger(object):
    @pytest.fixture()
    def trigger(self, timezone):
        return IntervalTrigger(seconds=1, start_date=datetime(2009, 8, 4, second=2), timezone=timezone)

    def test_invalid_interval(self, timezone):
        pytest.raises(TypeError, IntervalTrigger, '1-6', timezone=timezone)

    def test_before(self, trigger, timezone):
        """Tests that if "start_date" is later than "now", it will return start_date."""

        now = trigger.start_date - timedelta(seconds=2)
        assert trigger.get_next_fire_time(None, now) == trigger.start_date

    def test_within(self, trigger, timezone):
        """Tests that if "now" is between "start_date" and the next interval, it will return the next interval."""

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
        trigger = IntervalTrigger(minutes=5, start_date=start_date, end_date=datetime(2014, 5, 26, 0, 7),
                                  timezone=timezone)
        assert trigger.get_next_fire_time(None, start_date + timedelta(minutes=2)) == start_date.replace(minute=5)
        assert trigger.get_next_fire_time(None, start_date + timedelta(minutes=6)) is None

    def test_dst_change(self):
        """
        Making sure that IntervalTrigger works during the ambiguous "fall-back" DST period.
        Note that you should explicitly compare datetimes as strings to avoid the internal datetime comparison which
        would test for equality in the UTC timezone.
        """

        eastern = pytz.timezone('US/Eastern')
        start_date = datetime(2013, 6, 1)  # Start within EDT
        trigger = IntervalTrigger(hours=1, start_date=start_date, timezone=eastern)

        datetime_edt = eastern.localize(datetime(2013, 11, 3, 1, 5), is_dst=True)
        correct_next_date = eastern.normalize(datetime_edt + timedelta(minutes=55))
        assert str(trigger.get_next_fire_time(None, datetime_edt)) == str(correct_next_date)

        datetime_est = eastern.localize(datetime(2013, 11, 3, 1, 5), is_dst=False)
        correct_next_date = eastern.normalize(datetime_est + timedelta(minutes=55))
        assert str(trigger.get_next_fire_time(None, datetime_est)) == str(correct_next_date)

    def test_repr(self, trigger):
        assert repr(trigger) == \
            "<IntervalTrigger (interval=datetime.timedelta(0, 1), start_date='2009-08-04 00:00:02 CEST')>"

    def test_str(self, trigger):
        assert str(trigger) == "interval[0:00:01]"
