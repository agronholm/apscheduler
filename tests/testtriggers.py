from datetime import datetime

from nose.tools import eq_, raises
from dateutil.tz import tzoffset

from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger


local_tz = tzoffset('DUMMYTZ', 3600)


class TestCronTrigger(object):
    @classmethod
    def setup_class(cls):
        cls.defaults = {'timezone': local_tz}

    def test_cron_trigger_1(self):
        trigger = CronTrigger(self.defaults, year='2009/2', month='1/3', day='5-13')
        eq_(repr(trigger), "<CronTrigger (year='2009/2', month='1/3', day='5-13')>")
        eq_(str(trigger), "cron[year='2009/2', month='1/3', day='5-13']")
        start_date = datetime(2008, 12, 1, tzinfo=local_tz)
        correct_next_date = datetime(2009, 1, 5, tzinfo=local_tz)
        eq_(trigger.get_next_fire_time(start_date), correct_next_date)

    def test_cron_trigger_2(self):
        trigger = CronTrigger(self.defaults, year='2009/2', month='1/3', day='5-13')
        start_date = datetime(2009, 10, 14, tzinfo=local_tz)
        correct_next_date = datetime(2011, 1, 5, tzinfo=local_tz)
        eq_(trigger.get_next_fire_time(start_date), correct_next_date)

    def test_cron_trigger_3(self):
        trigger = CronTrigger(self.defaults, year='2009', month='2', hour='8-10')
        eq_(repr(trigger), "<CronTrigger (year='2009', month='2', hour='8-10')>")
        start_date = datetime(2009, 1, 1, tzinfo=local_tz)
        correct_next_date = datetime(2009, 2, 1, 8, tzinfo=local_tz)
        eq_(trigger.get_next_fire_time(start_date), correct_next_date)

    def test_cron_trigger_4(self):
        trigger = CronTrigger(self.defaults, year='2012', month='2', day='last')
        eq_(repr(trigger), "<CronTrigger (year='2012', month='2', day='last')>")
        start_date = datetime(2012, 2, 1, tzinfo=local_tz)
        correct_next_date = datetime(2012, 2, 29, tzinfo=local_tz)
        eq_(trigger.get_next_fire_time(start_date), correct_next_date)

    def test_cron_zero_value(self):
        trigger = CronTrigger(self.defaults, year=2009, month=2, hour=0)
        eq_(repr(trigger), "<CronTrigger (year='2009', month='2', hour='0')>")

    def test_cron_year_list(self):
        trigger = CronTrigger(self.defaults, year='2009,2008')
        eq_(repr(trigger), "<CronTrigger (year='2009,2008')>")
        eq_(str(trigger), "cron[year='2009,2008']")
        start_date = datetime(2009, 1, 1, tzinfo=local_tz)
        correct_next_date = datetime(2009, 1, 1, tzinfo=local_tz)
        eq_(trigger.get_next_fire_time(start_date), correct_next_date)

    def test_cron_start_date(self):
        trigger = CronTrigger(self.defaults, year='2009', month='2', hour='8-10',
                              start_date='2009-02-03 11:00:00')
        eq_(repr(trigger),
            "<CronTrigger (year='2009', month='2', hour='8-10', start_date='2009-02-03 11:00:00 DUMMYTZ')>")
        eq_(str(trigger), "cron[year='2009', month='2', hour='8-10']")
        start_date = datetime(2009, 1, 1, tzinfo=local_tz)
        correct_next_date = datetime(2009, 2, 4, 8, tzinfo=local_tz)
        eq_(trigger.get_next_fire_time(start_date), correct_next_date)

    def test_cron_weekday_overlap(self):
        trigger = CronTrigger(self.defaults, year=2009, month=1, day='6-10',
                              day_of_week='2-4')
        eq_(repr(trigger), "<CronTrigger (year='2009', month='1', day='6-10', day_of_week='2-4')>")
        eq_(str(trigger), "cron[year='2009', month='1', day='6-10', day_of_week='2-4']")
        start_date = datetime(2009, 1, 1, tzinfo=local_tz)
        correct_next_date = datetime(2009, 1, 7, tzinfo=local_tz)
        eq_(trigger.get_next_fire_time(start_date), correct_next_date)

    def test_cron_weekday_nomatch(self):
        trigger = CronTrigger(self.defaults, year=2009, month=1, day='6-10', day_of_week='0,6')
        eq_(repr(trigger), "<CronTrigger (year='2009', month='1', day='6-10', day_of_week='0,6')>")
        eq_(str(trigger), "cron[year='2009', month='1', day='6-10', day_of_week='0,6']")
        start_date = datetime(2009, 1, 1, tzinfo=local_tz)
        correct_next_date = None
        eq_(trigger.get_next_fire_time(start_date), correct_next_date)

    def test_cron_weekday_positional(self):
        trigger = CronTrigger(self.defaults, year=2009, month=1, day='4th wed')
        eq_(repr(trigger), "<CronTrigger (year='2009', month='1', day='4th wed')>")
        eq_(str(trigger), "cron[year='2009', month='1', day='4th wed']")
        start_date = datetime(2009, 1, 1, tzinfo=local_tz)
        correct_next_date = datetime(2009, 1, 28, tzinfo=local_tz)
        eq_(trigger.get_next_fire_time(start_date), correct_next_date)

    def test_week_1(self):
        trigger = CronTrigger(self.defaults, year=2009, month=2, week=8)
        eq_(repr(trigger), "<CronTrigger (year='2009', month='2', week='8')>")
        eq_(str(trigger), "cron[year='2009', month='2', week='8']")
        start_date = datetime(2009, 1, 1, tzinfo=local_tz)
        correct_next_date = datetime(2009, 2, 16, tzinfo=local_tz)
        eq_(trigger.get_next_fire_time(start_date), correct_next_date)

    def test_week_2(self):
        trigger = CronTrigger(self.defaults, year=2009, week=15, day_of_week=2)
        eq_(repr(trigger), "<CronTrigger (year='2009', week='15', day_of_week='2')>")
        eq_(str(trigger), "cron[year='2009', week='15', day_of_week='2']")
        start_date = datetime(2009, 1, 1, tzinfo=local_tz)
        correct_next_date = datetime(2009, 4, 8, tzinfo=local_tz)
        eq_(trigger.get_next_fire_time(start_date), correct_next_date)

    def test_cron_extra_coverage(self):
        # This test has no value other than patching holes in test coverage
        trigger = CronTrigger(self.defaults, day='6,8')
        eq_(repr(trigger), "<CronTrigger (day='6,8')>")
        eq_(str(trigger), "cron[day='6,8']")
        start_date = datetime(2009, 12, 31, tzinfo=local_tz)
        correct_next_date = datetime(2010, 1, 6, tzinfo=local_tz)
        eq_(trigger.get_next_fire_time(start_date), correct_next_date)

    @raises(ValueError)
    def test_cron_faulty_expr(self):
        CronTrigger(self.defaults, year='2009-fault')

    def test_cron_increment_weekday(self):
        # Makes sure that incrementing the weekday field in the process of
        # calculating the next matching date won't cause problems
        trigger = CronTrigger(self.defaults, hour='5-6')
        eq_(repr(trigger), "<CronTrigger (hour='5-6')>")
        eq_(str(trigger), "cron[hour='5-6']")
        start_date = datetime(2009, 9, 25, 7, tzinfo=local_tz)
        correct_next_date = datetime(2009, 9, 26, 5, tzinfo=local_tz)
        eq_(trigger.get_next_fire_time(start_date), correct_next_date)

    @raises(TypeError)
    def test_cron_bad_kwarg(self):
        CronTrigger(self.defaults, second=0, third=1)

    def test_different_tz(self):
        alter_tz = tzoffset('ALTERNATE', -3600)
        trigger = CronTrigger(self.defaults, year=2009, week=15, day_of_week=2)
        eq_(repr(trigger), "<CronTrigger (year='2009', week='15', day_of_week='2')>")
        eq_(str(trigger), "cron[year='2009', week='15', day_of_week='2']")
        start_date = datetime(2008, 12, 31, 22, tzinfo=alter_tz)
        correct_next_date = datetime(2009, 4, 8, tzinfo=local_tz)
        eq_(trigger.get_next_fire_time(start_date), correct_next_date)


class TestDateTrigger(object):
    @classmethod
    def setup_class(cls):
        cls.defaults = {'timezone': local_tz}

    def test_date_trigger_earlier(self):
        fire_date = datetime(2009, 7, 6, tzinfo=local_tz)
        trigger = DateTrigger(self.defaults, fire_date)
        eq_(repr(trigger), "<DateTrigger (run_date='2009-07-06 00:00:00 DUMMYTZ')>")
        eq_(str(trigger), "date[2009-07-06 00:00:00 DUMMYTZ]")
        start_date = datetime(2008, 12, 1, tzinfo=local_tz)
        eq_(trigger.get_next_fire_time(start_date), fire_date)

    def test_date_trigger_exact(self):
        fire_date = datetime(2009, 7, 6, tzinfo=local_tz)
        trigger = DateTrigger(self.defaults, fire_date)
        start_date = datetime(2009, 7, 6, tzinfo=local_tz)
        eq_(trigger.get_next_fire_time(start_date), fire_date)

    def test_date_trigger_later(self):
        fire_date = datetime(2009, 7, 6, tzinfo=local_tz)
        trigger = DateTrigger(self.defaults, fire_date)
        start_date = datetime(2009, 7, 7, tzinfo=local_tz)
        eq_(trigger.get_next_fire_time(start_date), None)

    def test_date_trigger_text(self):
        trigger = DateTrigger(self.defaults, '2009-7-6')
        start_date = datetime(2009, 7, 6, tzinfo=local_tz)
        eq_(trigger.get_next_fire_time(start_date), datetime(2009, 7, 6, tzinfo=local_tz))

    def test_different_tz(self):
        alter_tz = tzoffset('ALTERNATE', -3600)
        fire_date = datetime(2009, 7, 5, 22, tzinfo=alter_tz)
        trigger = DateTrigger(self.defaults, fire_date)
        start_date = datetime(2009, 7, 6, tzinfo=local_tz)
        eq_(trigger.get_next_fire_time(start_date), fire_date)


class TestIntervalTrigger(object):
    @classmethod
    def setup_class(cls):
        cls.defaults = {'timezone': local_tz}

    def setUp(self):
        self.trigger = IntervalTrigger(self.defaults, seconds=1, start_date=datetime(2009, 8, 4, second=2))

    @raises(TypeError)
    def test_interval_invalid_interval(self):
        IntervalTrigger(self.defaults, '1-6')

    def test_interval_repr(self):
        eq_(repr(self.trigger),
            "<IntervalTrigger (interval=datetime.timedelta(0, 1), start_date='2009-08-04 00:00:02 DUMMYTZ')>")
        eq_(str(self.trigger), "interval[0:00:01]")

    def test_interval_before(self):
        start_date = datetime(2009, 8, 4, tzinfo=local_tz)
        correct_next_date = datetime(2009, 8, 4, second=2, tzinfo=local_tz)
        eq_(self.trigger.get_next_fire_time(start_date), correct_next_date)

    def test_interval_within(self):
        start_date = datetime(2009, 8, 4, second=2, microsecond=1000, tzinfo=local_tz)
        correct_next_date = datetime(2009, 8, 4, second=3, tzinfo=local_tz)
        eq_(self.trigger.get_next_fire_time(start_date), correct_next_date)

    def test_different_tz(self):
        alter_tz = tzoffset('ALTERNATE', -3600)
        start_date = datetime(2009, 8, 3, 22, second=2, microsecond=1000, tzinfo=alter_tz)
        correct_next_date = datetime(2009, 8, 4, second=3, tzinfo=local_tz)
        eq_(self.trigger.get_next_fire_time(start_date), correct_next_date)
