from datetime import datetime

from dateutil.tz import tzoffset
import pytest

from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

local_tz = tzoffset('DUMMYTZ', 3600)


class TestCronTrigger(object):
    def test_cron_trigger_1(self):
        trigger = CronTrigger(local_tz, year='2009/2', month='1/3', day='5-13')
        assert repr(trigger) == "<CronTrigger (year='2009/2', month='1/3', day='5-13')>"
        assert str(trigger) == "cron[year='2009/2', month='1/3', day='5-13']"
        start_date = datetime(2008, 12, 1, tzinfo=local_tz)
        correct_next_date = datetime(2009, 1, 5, tzinfo=local_tz)
        assert trigger.get_next_fire_time(start_date) == correct_next_date

    def test_cron_trigger_2(self):
        trigger = CronTrigger(local_tz, year='2009/2', month='1/3', day='5-13')
        start_date = datetime(2009, 10, 14, tzinfo=local_tz)
        correct_next_date = datetime(2011, 1, 5, tzinfo=local_tz)
        assert trigger.get_next_fire_time(start_date) == correct_next_date

    def test_cron_trigger_3(self):
        trigger = CronTrigger(local_tz, year='2009', month='2', hour='8-10')
        assert repr(trigger) == "<CronTrigger (year='2009', month='2', hour='8-10')>"
        start_date = datetime(2009, 1, 1, tzinfo=local_tz)
        correct_next_date = datetime(2009, 2, 1, 8, tzinfo=local_tz)
        assert trigger.get_next_fire_time(start_date) == correct_next_date

    def test_cron_trigger_4(self):
        trigger = CronTrigger(local_tz, year='2012', month='2', day='last')
        assert repr(trigger) == "<CronTrigger (year='2012', month='2', day='last')>"
        start_date = datetime(2012, 2, 1, tzinfo=local_tz)
        correct_next_date = datetime(2012, 2, 29, tzinfo=local_tz)
        assert trigger.get_next_fire_time(start_date) == correct_next_date

    def test_cron_zero_value(self):
        trigger = CronTrigger(local_tz, year=2009, month=2, hour=0)
        assert repr(trigger) == "<CronTrigger (year='2009', month='2', hour='0')>"

    def test_cron_year_list(self):
        trigger = CronTrigger(local_tz, year='2009,2008')
        assert repr(trigger) == "<CronTrigger (year='2009,2008')>"
        assert str(trigger) == "cron[year='2009,2008']"
        start_date = datetime(2009, 1, 1, tzinfo=local_tz)
        correct_next_date = datetime(2009, 1, 1, tzinfo=local_tz)
        assert trigger.get_next_fire_time(start_date) == correct_next_date

    def test_cron_start_date(self):
        trigger = CronTrigger(local_tz, year='2009', month='2', hour='8-10',
                              start_date='2009-02-03 11:00:00')
        assert repr(trigger) == \
            "<CronTrigger (year='2009', month='2', hour='8-10', start_date='2009-02-03 11:00:00 DUMMYTZ')>"
        assert str(trigger) == "cron[year='2009', month='2', hour='8-10']"
        start_date = datetime(2009, 1, 1, tzinfo=local_tz)
        correct_next_date = datetime(2009, 2, 4, 8, tzinfo=local_tz)
        assert trigger.get_next_fire_time(start_date) == correct_next_date

    def test_cron_weekday_overlap(self):
        trigger = CronTrigger(local_tz, year=2009, month=1, day='6-10',
                              day_of_week='2-4')
        assert repr(trigger) == "<CronTrigger (year='2009', month='1', day='6-10', day_of_week='2-4')>"
        assert str(trigger) == "cron[year='2009', month='1', day='6-10', day_of_week='2-4']"
        start_date = datetime(2009, 1, 1, tzinfo=local_tz)
        correct_next_date = datetime(2009, 1, 7, tzinfo=local_tz)
        assert trigger.get_next_fire_time(start_date) == correct_next_date

    def test_cron_weekday_nomatch(self):
        trigger = CronTrigger(local_tz, year=2009, month=1, day='6-10', day_of_week='0,6')
        assert repr(trigger) == "<CronTrigger (year='2009', month='1', day='6-10', day_of_week='0,6')>"
        assert str(trigger) == "cron[year='2009', month='1', day='6-10', day_of_week='0,6']"
        start_date = datetime(2009, 1, 1, tzinfo=local_tz)
        correct_next_date = None
        assert trigger.get_next_fire_time(start_date) == correct_next_date

    def test_cron_weekday_positional(self):
        trigger = CronTrigger(local_tz, year=2009, month=1, day='4th wed')
        assert repr(trigger) == "<CronTrigger (year='2009', month='1', day='4th wed')>"
        assert str(trigger) == "cron[year='2009', month='1', day='4th wed']"
        start_date = datetime(2009, 1, 1, tzinfo=local_tz)
        correct_next_date = datetime(2009, 1, 28, tzinfo=local_tz)
        assert trigger.get_next_fire_time(start_date) == correct_next_date

    def test_week_1(self):
        trigger = CronTrigger(local_tz, year=2009, month=2, week=8)
        assert repr(trigger) == "<CronTrigger (year='2009', month='2', week='8')>"
        assert str(trigger) == "cron[year='2009', month='2', week='8']"
        start_date = datetime(2009, 1, 1, tzinfo=local_tz)
        correct_next_date = datetime(2009, 2, 16, tzinfo=local_tz)
        assert trigger.get_next_fire_time(start_date) == correct_next_date

    def test_week_2(self):
        trigger = CronTrigger(local_tz, year=2009, week=15, day_of_week=2)
        assert repr(trigger) == "<CronTrigger (year='2009', week='15', day_of_week='2')>"
        assert str(trigger) == "cron[year='2009', week='15', day_of_week='2']"
        start_date = datetime(2009, 1, 1, tzinfo=local_tz)
        correct_next_date = datetime(2009, 4, 8, tzinfo=local_tz)
        assert trigger.get_next_fire_time(start_date) == correct_next_date

    def test_cron_extra_coverage(self):
        # This test has no value other than patching holes in test coverage
        trigger = CronTrigger(local_tz, day='6,8')
        assert repr(trigger) == "<CronTrigger (day='6,8')>"
        assert str(trigger) == "cron[day='6,8']"
        start_date = datetime(2009, 12, 31, tzinfo=local_tz)
        correct_next_date = datetime(2010, 1, 6, tzinfo=local_tz)
        assert trigger.get_next_fire_time(start_date) == correct_next_date

    def test_cron_faulty_expr(self):
        pytest.raises(ValueError, CronTrigger, local_tz, year='2009-fault')

    def test_cron_increment_weekday(self):
        # Makes sure that incrementing the weekday field in the process of
        # calculating the next matching date won't cause problems
        trigger = CronTrigger(local_tz, hour='5-6')
        assert repr(trigger) == "<CronTrigger (hour='5-6')>"
        assert str(trigger) == "cron[hour='5-6']"
        start_date = datetime(2009, 9, 25, 7, tzinfo=local_tz)
        correct_next_date = datetime(2009, 9, 26, 5, tzinfo=local_tz)
        assert trigger.get_next_fire_time(start_date) == correct_next_date

    def test_cron_bad_kwarg(self):
        pytest.raises(TypeError, CronTrigger, local_tz, second=0, third=1)

    def test_different_tz(self):
        alter_tz = tzoffset('ALTERNATE', -3600)
        trigger = CronTrigger(local_tz, year=2009, week=15, day_of_week=2)
        assert repr(trigger) == "<CronTrigger (year='2009', week='15', day_of_week='2')>"
        assert str(trigger) == "cron[year='2009', week='15', day_of_week='2']"
        start_date = datetime(2008, 12, 31, 22, tzinfo=alter_tz)
        correct_next_date = datetime(2009, 4, 8, tzinfo=local_tz)
        assert trigger.get_next_fire_time(start_date) == correct_next_date


class TestDateTrigger(object):
    def test_date_trigger_earlier(self):
        fire_date = datetime(2009, 7, 6, tzinfo=local_tz)
        trigger = DateTrigger(local_tz, fire_date)
        assert repr(trigger) == "<DateTrigger (run_date='2009-07-06 00:00:00 DUMMYTZ')>"
        assert str(trigger) == "date[2009-07-06 00:00:00 DUMMYTZ]"
        start_date = datetime(2008, 12, 1, tzinfo=local_tz)
        assert trigger.get_next_fire_time(start_date) == fire_date

    def test_date_trigger_exact(self):
        fire_date = datetime(2009, 7, 6, tzinfo=local_tz)
        trigger = DateTrigger(local_tz, fire_date)
        start_date = datetime(2009, 7, 6, tzinfo=local_tz)
        assert trigger.get_next_fire_time(start_date) == fire_date

    def test_date_trigger_later(self):
        fire_date = datetime(2009, 7, 6, tzinfo=local_tz)
        trigger = DateTrigger(local_tz, fire_date)
        start_date = datetime(2009, 7, 7, tzinfo=local_tz)
        assert trigger.get_next_fire_time(start_date) is None

    def test_date_trigger_text(self):
        trigger = DateTrigger(local_tz, '2009-7-6')
        start_date = datetime(2009, 7, 6, tzinfo=local_tz)
        assert trigger.get_next_fire_time(start_date) == datetime(2009, 7, 6, tzinfo=local_tz)

    def test_different_tz(self):
        alter_tz = tzoffset('ALTERNATE', -3600)
        fire_date = datetime(2009, 7, 5, 22, tzinfo=alter_tz)
        trigger = DateTrigger(local_tz, fire_date)
        start_date = datetime(2009, 7, 6, tzinfo=local_tz)
        assert trigger.get_next_fire_time(start_date) == fire_date


class TestIntervalTrigger(object):
    @pytest.fixture()
    def trigger(self):
        return IntervalTrigger(local_tz, seconds=1, start_date=datetime(2009, 8, 4, second=2))

    def test_interval_invalid_interval(self):
        pytest.raises(TypeError, IntervalTrigger, local_tz, '1-6')

    def test_interval_repr(self, trigger):
        assert repr(trigger) == \
            "<IntervalTrigger (interval=datetime.timedelta(0, 1), start_date='2009-08-04 00:00:02 DUMMYTZ')>"
        assert str(trigger) == "interval[0:00:01]"

    def test_interval_before(self, trigger):
        start_date = datetime(2009, 8, 4, tzinfo=local_tz)
        correct_next_date = datetime(2009, 8, 4, second=2, tzinfo=local_tz)
        assert trigger.get_next_fire_time(start_date) == correct_next_date

    def test_interval_within(self, trigger):
        start_date = datetime(2009, 8, 4, second=2, microsecond=1000, tzinfo=local_tz)
        correct_next_date = datetime(2009, 8, 4, second=3, tzinfo=local_tz)
        assert trigger.get_next_fire_time(start_date) == correct_next_date

    def test_different_tz(self, trigger):
        alter_tz = tzoffset('ALTERNATE', -3600)
        start_date = datetime(2009, 8, 3, 22, second=2, microsecond=1000, tzinfo=alter_tz)
        correct_next_date = datetime(2009, 8, 4, second=3, tzinfo=local_tz)
        assert trigger.get_next_fire_time(start_date) == correct_next_date
