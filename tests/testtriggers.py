from datetime import datetime, timedelta

from nose.tools import eq_, raises

from apscheduler.triggers import CronTrigger, SimpleTrigger, IntervalTrigger


def test_cron_trigger_1():
    trigger = CronTrigger(year='2009/2', month='1/3', day='5-13')
    eq_(repr(trigger), "<CronTrigger (year='2009/2', month='1/3', day='5-13')>")
    eq_(str(trigger), "cron[year='2009/2', month='1/3', day='5-13']")
    start_date = datetime(2008, 12, 1)
    correct_next_date = datetime(2009, 1, 5)
    eq_(trigger.get_next_fire_time(start_date), correct_next_date)


def test_cron_trigger_2():
    trigger = CronTrigger(year='2009/2', month='1/3', day='5-13')
    start_date = datetime(2009, 10, 14)
    correct_next_date = datetime(2011, 1, 5)
    eq_(trigger.get_next_fire_time(start_date), correct_next_date)


def test_cron_trigger_3():
    trigger = CronTrigger(year='2009', month='2', hour='8-10')
    eq_(repr(trigger), "<CronTrigger (year='2009', month='2', hour='8-10')>")
    start_date = datetime(2009, 1, 1)
    correct_next_date = datetime(2009, 2, 1, 8)
    eq_(trigger.get_next_fire_time(start_date), correct_next_date)


def test_cron_trigger_4():
    trigger = CronTrigger(year='2012', month='2', day='last')
    eq_(repr(trigger), "<CronTrigger (year='2012', month='2', day='last')>")
    start_date = datetime(2012, 2, 1)
    correct_next_date = datetime(2012, 2, 29)
    eq_(trigger.get_next_fire_time(start_date), correct_next_date)


def test_cron_zero_value():
    trigger = CronTrigger(year=2009, month=2, hour=0)
    eq_(repr(trigger), "<CronTrigger (year='2009', month='2', hour='0')>")


def test_cron_year_list():
    trigger = CronTrigger(year='2009,2008')
    eq_(repr(trigger), "<CronTrigger (year='2009,2008')>")
    eq_(str(trigger), "cron[year='2009,2008']")
    start_date = datetime(2009, 1, 1)
    correct_next_date = datetime(2009, 1, 1)
    eq_(trigger.get_next_fire_time(start_date), correct_next_date)


def test_cron_start_date():
    trigger = CronTrigger(year='2009', month='2', hour='8-10',
                          start_date='2009-02-03 11:00:00')
    eq_(repr(trigger), "<CronTrigger (year='2009', month='2', hour='8-10', start_date='2009-02-03 11:00:00')>")
    eq_(str(trigger), "cron[year='2009', month='2', hour='8-10']")
    start_date = datetime(2009, 1, 1)
    correct_next_date = datetime(2009, 2, 4, 8)
    eq_(trigger.get_next_fire_time(start_date), correct_next_date)


def test_cron_weekday_overlap():
    trigger = CronTrigger(year=2009, month=1, day='6-10',
                          day_of_week='2-4')
    eq_(repr(trigger), "<CronTrigger (year='2009', month='1', day='6-10', day_of_week='2-4')>")
    eq_(str(trigger), "cron[year='2009', month='1', day='6-10', day_of_week='2-4']")
    start_date = datetime(2009, 1, 1)
    correct_next_date = datetime(2009, 1, 7)
    eq_(trigger.get_next_fire_time(start_date), correct_next_date)


def test_cron_weekday_nomatch():
    trigger = CronTrigger(year=2009, month=1, day='6-10', day_of_week='0,6')
    eq_(repr(trigger), "<CronTrigger (year='2009', month='1', day='6-10', day_of_week='0,6')>")
    eq_(str(trigger), "cron[year='2009', month='1', day='6-10', day_of_week='0,6']")
    start_date = datetime(2009, 1, 1)
    correct_next_date = None
    eq_(trigger.get_next_fire_time(start_date), correct_next_date)


def test_cron_weekday_positional():
    trigger = CronTrigger(year=2009, month=1, day='4th wed')
    eq_(repr(trigger), "<CronTrigger (year='2009', month='1', day='4th wed')>")
    eq_(str(trigger), "cron[year='2009', month='1', day='4th wed']")
    start_date = datetime(2009, 1, 1)
    correct_next_date = datetime(2009, 1, 28)
    eq_(trigger.get_next_fire_time(start_date), correct_next_date)


def test_week_1():
    trigger = CronTrigger(year=2009, month=2, week=8)
    eq_(repr(trigger), "<CronTrigger (year='2009', month='2', week='8')>")
    eq_(str(trigger), "cron[year='2009', month='2', week='8']")
    start_date = datetime(2009, 1, 1)
    correct_next_date = datetime(2009, 2, 16)
    eq_(trigger.get_next_fire_time(start_date), correct_next_date)


def test_week_2():
    trigger = CronTrigger(year=2009, week=15, day_of_week=2)
    eq_(repr(trigger), "<CronTrigger (year='2009', week='15', day_of_week='2')>")
    eq_(str(trigger), "cron[year='2009', week='15', day_of_week='2']")
    start_date = datetime(2009, 1, 1)
    correct_next_date = datetime(2009, 4, 8)
    eq_(trigger.get_next_fire_time(start_date), correct_next_date)


def test_cron_extra_coverage():
    # This test has no value other than patching holes in test coverage
    trigger = CronTrigger(day='6,8')
    eq_(repr(trigger), "<CronTrigger (day='6,8')>")
    eq_(str(trigger), "cron[day='6,8']")
    start_date = datetime(2009, 12, 31)
    correct_next_date = datetime(2010, 1, 6)
    eq_(trigger.get_next_fire_time(start_date), correct_next_date)


@raises(ValueError)
def test_cron_faulty_expr():
    CronTrigger(year='2009-fault')


def test_cron_increment_weekday():
    # Makes sure that incrementing the weekday field in the process of
    # calculating the next matching date won't cause problems
    trigger = CronTrigger(hour='5-6')
    eq_(repr(trigger), "<CronTrigger (hour='5-6')>")
    eq_(str(trigger), "cron[hour='5-6']")
    start_date = datetime(2009, 9, 25, 7)
    correct_next_date = datetime(2009, 9, 26, 5)
    eq_(trigger.get_next_fire_time(start_date), correct_next_date)


@raises(TypeError)
def test_cron_bad_kwarg():
    CronTrigger(second=0, third=1)


def test_date_trigger_earlier():
    fire_date = datetime(2009, 7, 6)
    trigger = SimpleTrigger(fire_date)
    eq_(repr(trigger), "<SimpleTrigger (run_date=datetime.datetime(2009, 7, 6, 0, 0))>")
    eq_(str(trigger), "date[2009-07-06 00:00:00]")
    start_date = datetime(2008, 12, 1)
    eq_(trigger.get_next_fire_time(start_date), fire_date)


def test_date_trigger_exact():
    fire_date = datetime(2009, 7, 6)
    trigger = SimpleTrigger(fire_date)
    start_date = datetime(2009, 7, 6)
    eq_(trigger.get_next_fire_time(start_date), fire_date)


def test_date_trigger_later():
    fire_date = datetime(2009, 7, 6)
    trigger = SimpleTrigger(fire_date)
    start_date = datetime(2009, 7, 7)
    eq_(trigger.get_next_fire_time(start_date), None)


def test_date_trigger_text():
    trigger = SimpleTrigger('2009-7-6')
    start_date = datetime(2009, 7, 6)
    eq_(trigger.get_next_fire_time(start_date), datetime(2009, 7, 6))


@raises(TypeError)
def test_interval_invalid_interval():
    IntervalTrigger('1-6')


class TestInterval(object):
    def setUp(self):
        interval = timedelta(seconds=1)
        trigger_start_date = datetime(2009, 8, 4, second=2)
        self.trigger = IntervalTrigger(interval, trigger_start_date)

    def test_interval_repr(self):
        eq_(repr(self.trigger),
            "<IntervalTrigger (interval=datetime.timedelta(0, 1), start_date=datetime.datetime(2009, 8, 4, 0, 0, 2))>")
        eq_(str(self.trigger), "interval[0:00:01]")

    def test_interval_before(self):
        start_date = datetime(2009, 8, 4)
        correct_next_date = datetime(2009, 8, 4, second=2)
        eq_(self.trigger.get_next_fire_time(start_date), correct_next_date)

    def test_interval_within(self):
        start_date = datetime(2009, 8, 4, second=2, microsecond=1000)
        correct_next_date = datetime(2009, 8, 4, second=3)
        eq_(self.trigger.get_next_fire_time(start_date), correct_next_date)
