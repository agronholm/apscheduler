from datetime import datetime, timedelta

from dateutil.tz import tzoffset
import pytz
import pytest

from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger


class TestCronTrigger(object):
    def test_cron_trigger_1(self, timezone):
        trigger = CronTrigger(year='2009/2', month='1/3', day='5-13', timezone=timezone)
        assert repr(trigger) == "<CronTrigger (year='2009/2', month='1/3', day='5-13')>"
        assert str(trigger) == "cron[year='2009/2', month='1/3', day='5-13']"
        start_date = datetime(2008, 12, 1, tzinfo=timezone)
        correct_next_date = datetime(2009, 1, 5, tzinfo=timezone)
        assert trigger.get_next_fire_time(start_date) == correct_next_date

    def test_cron_trigger_2(self, timezone):
        trigger = CronTrigger(year='2009/2', month='1/3', day='5-13', timezone=timezone)
        start_date = datetime(2009, 10, 14, tzinfo=timezone)
        correct_next_date = datetime(2011, 1, 5, tzinfo=timezone)
        assert trigger.get_next_fire_time(start_date) == correct_next_date

    def test_cron_trigger_3(self, timezone):
        trigger = CronTrigger(year='2009', month='2', hour='8-10', timezone=timezone)
        assert repr(trigger) == "<CronTrigger (year='2009', month='2', hour='8-10')>"
        start_date = datetime(2009, 1, 1, tzinfo=timezone)
        correct_next_date = datetime(2009, 2, 1, 8, tzinfo=timezone)
        assert trigger.get_next_fire_time(start_date) == correct_next_date

    def test_cron_trigger_4(self, timezone):
        trigger = CronTrigger(year='2012', month='2', day='last', timezone=timezone)
        assert repr(trigger) == "<CronTrigger (year='2012', month='2', day='last')>"
        start_date = datetime(2012, 2, 1, tzinfo=timezone)
        correct_next_date = datetime(2012, 2, 29, tzinfo=timezone)
        assert trigger.get_next_fire_time(start_date) == correct_next_date

    def test_cron_zero_value(self, timezone):
        trigger = CronTrigger(year=2009, month=2, hour=0, timezone=timezone)
        assert repr(trigger) == "<CronTrigger (year='2009', month='2', hour='0')>"

    def test_cron_year_list(self, timezone):
        trigger = CronTrigger(year='2009,2008', timezone=timezone)
        assert repr(trigger) == "<CronTrigger (year='2009,2008')>"
        assert str(trigger) == "cron[year='2009,2008']"
        start_date = datetime(2009, 1, 1, tzinfo=timezone)
        correct_next_date = datetime(2009, 1, 1, tzinfo=timezone)
        assert trigger.get_next_fire_time(start_date) == correct_next_date

    def test_cron_start_date(self, timezone):
        trigger = CronTrigger(year='2009', month='2', hour='8-10', start_date='2009-02-03 11:00:00', timezone=timezone)
        assert repr(trigger) == \
            "<CronTrigger (year='2009', month='2', hour='8-10', start_date='2009-02-03 11:00:00 DUMMYTZ')>"
        assert str(trigger) == "cron[year='2009', month='2', hour='8-10']"
        start_date = datetime(2009, 1, 1, tzinfo=timezone)
        correct_next_date = datetime(2009, 2, 4, 8, tzinfo=timezone)
        assert trigger.get_next_fire_time(start_date) == correct_next_date

    def test_cron_weekday_overlap(self, timezone):
        trigger = CronTrigger(year=2009, month=1, day='6-10', day_of_week='2-4', timezone=timezone)
        assert repr(trigger) == "<CronTrigger (year='2009', month='1', day='6-10', day_of_week='2-4')>"
        assert str(trigger) == "cron[year='2009', month='1', day='6-10', day_of_week='2-4']"
        start_date = datetime(2009, 1, 1, tzinfo=timezone)
        correct_next_date = datetime(2009, 1, 7, tzinfo=timezone)
        assert trigger.get_next_fire_time(start_date) == correct_next_date

    def test_cron_weekday_nomatch(self, timezone):
        trigger = CronTrigger(year=2009, month=1, day='6-10', day_of_week='0,6', timezone=timezone)
        assert repr(trigger) == "<CronTrigger (year='2009', month='1', day='6-10', day_of_week='0,6')>"
        assert str(trigger) == "cron[year='2009', month='1', day='6-10', day_of_week='0,6']"
        start_date = datetime(2009, 1, 1, tzinfo=timezone)
        correct_next_date = None
        assert trigger.get_next_fire_time(start_date) == correct_next_date

    def test_cron_weekday_positional(self, timezone):
        trigger = CronTrigger(year=2009, month=1, day='4th wed', timezone=timezone)
        assert repr(trigger) == "<CronTrigger (year='2009', month='1', day='4th wed')>"
        assert str(trigger) == "cron[year='2009', month='1', day='4th wed']"
        start_date = datetime(2009, 1, 1, tzinfo=timezone)
        correct_next_date = datetime(2009, 1, 28, tzinfo=timezone)
        assert trigger.get_next_fire_time(start_date) == correct_next_date

    def test_week_1(self, timezone):
        trigger = CronTrigger(year=2009, month=2, week=8, timezone=timezone)
        assert repr(trigger) == "<CronTrigger (year='2009', month='2', week='8')>"
        assert str(trigger) == "cron[year='2009', month='2', week='8']"
        start_date = datetime(2009, 1, 1, tzinfo=timezone)
        correct_next_date = datetime(2009, 2, 16, tzinfo=timezone)
        assert trigger.get_next_fire_time(start_date) == correct_next_date

    def test_week_2(self, timezone):
        trigger = CronTrigger(year=2009, week=15, day_of_week=2, timezone=timezone)
        assert repr(trigger) == "<CronTrigger (year='2009', week='15', day_of_week='2')>"
        assert str(trigger) == "cron[year='2009', week='15', day_of_week='2']"
        start_date = datetime(2009, 1, 1, tzinfo=timezone)
        correct_next_date = datetime(2009, 4, 8, tzinfo=timezone)
        assert trigger.get_next_fire_time(start_date) == correct_next_date

    def test_cron_extra_coverage(self, timezone):
        # This test has no value other than patching holes in test coverage
        trigger = CronTrigger(day='6,8', timezone=timezone)
        assert repr(trigger) == "<CronTrigger (day='6,8')>"
        assert str(trigger) == "cron[day='6,8']"
        start_date = datetime(2009, 12, 31, tzinfo=timezone)
        correct_next_date = datetime(2010, 1, 6, tzinfo=timezone)
        assert trigger.get_next_fire_time(start_date) == correct_next_date

    def test_cron_faulty_expr(self, timezone):
        pytest.raises(ValueError, CronTrigger, year='2009-fault', timezone=timezone)

    def test_cron_increment_weekday(self, timezone):
        # Makes sure that incrementing the weekday field in the process of
        # calculating the next matching date won't cause problems
        trigger = CronTrigger(hour='5-6', timezone=timezone)
        assert repr(trigger) == "<CronTrigger (hour='5-6')>"
        assert str(trigger) == "cron[hour='5-6']"
        start_date = datetime(2009, 9, 25, 7, tzinfo=timezone)
        correct_next_date = datetime(2009, 9, 26, 5, tzinfo=timezone)
        assert trigger.get_next_fire_time(start_date) == correct_next_date

    def test_cron_bad_kwarg(self, timezone):
        pytest.raises(TypeError, CronTrigger, second=0, third=1, timezone=timezone)

    def test_timezone_from_start_date(self, timezone):
        start_date = datetime(2014, 4, 13, 5, 30, tzinfo=timezone)
        trigger = CronTrigger(year=2014, hour=4, start_date=start_date)
        assert trigger.timezone == timezone

    def test_different_tz(self, timezone):
        alter_tz = tzoffset('ALTERNATE', -3600)
        trigger = CronTrigger(year=2009, week=15, day_of_week=2, timezone=timezone)
        assert repr(trigger) == "<CronTrigger (year='2009', week='15', day_of_week='2')>"
        assert str(trigger) == "cron[year='2009', week='15', day_of_week='2']"
        start_date = datetime(2008, 12, 31, 22, tzinfo=alter_tz)
        correct_next_date = datetime(2009, 4, 8, tzinfo=timezone)
        assert trigger.get_next_fire_time(start_date) == correct_next_date

    def test_dst_change(self):
        """
        Making sure that CronTrigger works during the ambiguous
        "fall-back" DST period. Note that you should explicitly
        compare datetimes as strings to avoid the internal datetime
        comparison which would test for equality in the UTC timezone.
        """
        eastern = pytz.timezone('US/Eastern')
        trigger = CronTrigger(hour='*/1', timezone=eastern)

        datetime_edt = eastern.localize(datetime(2013, 11, 3, 1, 5), is_dst=True)
        correct_next_date = eastern.normalize(datetime_edt + timedelta(minutes=55))
        assert str(trigger.get_next_fire_time(datetime_edt)) == str(correct_next_date)

        datetime_est = eastern.localize(datetime(2013, 11, 3, 1, 5), is_dst=False)
        correct_next_date = eastern.normalize(datetime_est + timedelta(minutes=55))
        assert str(trigger.get_next_fire_time(datetime_est)) == str(correct_next_date)

    def test_timezone_change(self):
        """
        Ensuring that get_next_fire_time method returns datetimes in the
        timezone of the trigger and not in the timezone of the passed in
        start_date.
        """
        est = tzoffset('EST', -18000)
        cst = tzoffset('CST', -21600)
        trigger = CronTrigger(hour=11, minute='*/5', timezone=est)
        start_date = datetime(2009, 9, 26, 10, 16, tzinfo=cst)
        correct_next_date = datetime(2009, 9, 26, 11, 20, tzinfo=est)
        assert str(trigger.get_next_fire_time(start_date)) == str(correct_next_date)


class TestDateTrigger(object):
    def test_date_trigger_earlier(self, timezone):
        fire_date = datetime(2009, 7, 6, tzinfo=timezone)
        trigger = DateTrigger(fire_date, timezone)
        assert repr(trigger) == "<DateTrigger (run_date='2009-07-06 00:00:00 DUMMYTZ')>"
        assert str(trigger) == "date[2009-07-06 00:00:00 DUMMYTZ]"
        start_date = datetime(2008, 12, 1, tzinfo=timezone)
        assert trigger.get_next_fire_time(start_date) == fire_date

    def test_date_trigger_exact(self, timezone):
        fire_date = datetime(2009, 7, 6, tzinfo=timezone)
        trigger = DateTrigger(fire_date, timezone)
        start_date = datetime(2009, 7, 6, tzinfo=timezone)
        assert trigger.get_next_fire_time(start_date) == fire_date

    def test_date_trigger_later(self, timezone):
        fire_date = datetime(2009, 7, 6, tzinfo=timezone)
        trigger = DateTrigger(fire_date, timezone)
        start_date = datetime(2009, 7, 7, tzinfo=timezone)
        assert trigger.get_next_fire_time(start_date) is None

    def test_date_trigger_text(self, timezone):
        trigger = DateTrigger('2009-7-6', timezone)
        start_date = datetime(2009, 7, 6, tzinfo=timezone)
        assert trigger.get_next_fire_time(start_date) == datetime(2009, 7, 6, tzinfo=timezone)

    def test_different_tz(self, timezone):
        alter_tz = tzoffset('ALTERNATE', -3600)
        fire_date = datetime(2009, 7, 5, 22, tzinfo=alter_tz)
        trigger = DateTrigger(fire_date, timezone)
        start_date = datetime(2009, 7, 6, tzinfo=timezone)
        assert trigger.get_next_fire_time(start_date) == fire_date

    def test_dst_change(self):
        """
        Making sure that DateTrigger works during the ambiguous
        "fall-back" DST period. Note that you should explicitly
        compare datetimes as strings to avoid the internal datetime
        comparison which would test for equality in the UTC timezone.
        """
        eastern = pytz.timezone('US/Eastern')
        datetime_edt = eastern.localize(datetime(2013, 11, 3, 1, 5), is_dst=True)
        datetime_est = eastern.localize(datetime(2013, 11, 3, 1, 5), is_dst=False)

        fire_date = datetime_edt + timedelta(minutes=55)
        trigger = DateTrigger(run_date=fire_date, timezone=eastern)
        correct_next_date = eastern.normalize(fire_date)
        assert str(trigger.get_next_fire_time(datetime_edt)) == str(correct_next_date)

        fire_date = datetime_est + timedelta(minutes=55)
        trigger = DateTrigger(run_date=fire_date, timezone=eastern)
        correct_next_date = eastern.normalize(fire_date)
        assert str(trigger.get_next_fire_time(datetime_est)) == str(correct_next_date)


class TestIntervalTrigger(object):
    @pytest.fixture()
    def trigger(self, timezone):
        return IntervalTrigger(seconds=1, start_date=datetime(2009, 8, 4, second=2), timezone=timezone)

    def test_interval_invalid_interval(self, timezone):
        pytest.raises(TypeError, IntervalTrigger, '1-6', timezone=timezone)

    def test_interval_repr(self, trigger, timezone):
        assert repr(trigger) == \
            "<IntervalTrigger (interval=datetime.timedelta(0, 1), start_date='2009-08-04 00:00:02 DUMMYTZ')>"
        assert str(trigger) == "interval[0:00:01]"

    def test_interval_before(self, trigger, timezone):
        start_date = datetime(2009, 8, 4, tzinfo=timezone)
        correct_next_date = datetime(2009, 8, 4, second=2, tzinfo=timezone)
        assert trigger.get_next_fire_time(start_date) == correct_next_date

    def test_interval_within(self, trigger, timezone):
        start_date = datetime(2009, 8, 4, second=2, microsecond=1000, tzinfo=timezone)
        correct_next_date = datetime(2009, 8, 4, second=3, tzinfo=timezone)
        assert trigger.get_next_fire_time(start_date) == correct_next_date

    def test_different_tz(self, trigger, timezone):
        alter_tz = tzoffset('ALTERNATE', -3600)
        start_date = datetime(2009, 8, 3, 22, second=2, microsecond=1000, tzinfo=alter_tz)
        correct_next_date = datetime(2009, 8, 4, second=3, tzinfo=timezone)
        assert trigger.get_next_fire_time(start_date) == correct_next_date

    def test_dst_change(self):
        """
        Making sure that IntervalTrigger works during the ambiguous
        "fall-back" DST period. Note that you should explicitly
        compare datetimes as strings to avoid the internal datetime
        comparison which would test for equality in the UTC timezone.
        """
        eastern = pytz.timezone('US/Eastern')
        start_date = datetime(2013, 6, 1)  # Start within EDT
        trigger = IntervalTrigger(hours=1, start_date=start_date, timezone=eastern)

        datetime_edt = eastern.localize(datetime(2013, 11, 3, 1, 5), is_dst=True)
        correct_next_date = eastern.normalize(datetime_edt + timedelta(minutes=55))
        assert str(trigger.get_next_fire_time(datetime_edt)) == str(correct_next_date)

        datetime_est = eastern.localize(datetime(2013, 11, 3, 1, 5), is_dst=False)
        correct_next_date = eastern.normalize(datetime_est + timedelta(minutes=55))
        assert str(trigger.get_next_fire_time(datetime_est)) == str(correct_next_date)
