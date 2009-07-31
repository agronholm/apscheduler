from datetime import datetime, timedelta

from nose.tools import eq_, raises

from apscheduler.triggers import *


def test_cron_trigger_1():
    trigger = CronTrigger(year='2009/2', month='1/3', day='5-13')
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
    start_date = datetime(2009, 1, 1)
    correct_next_date = datetime(2009, 2, 1, 8)
    eq_(trigger.get_next_fire_time(start_date), correct_next_date)

def test_cron_weekday_overlap():
    trigger = CronTrigger(year=2009, month=1, day='6-10', day_of_week='2-4')
    start_date = datetime(2009, 1, 1)
    correct_next_date = datetime(2009, 1, 7)
    eq_(trigger.get_next_fire_time(start_date), correct_next_date)

def test_cron_weekday_nomatch():
    trigger = CronTrigger(year=2009, month=1, day='6-10', day_of_week='0,6')
    start_date = datetime(2009, 1, 1)
    correct_next_date = None
    eq_(trigger.get_next_fire_time(start_date), correct_next_date)

def test_cron_extra_coverage():
    # This test has no value other than patching holes in test coverage
    trigger = CronTrigger(day='6,8')
    start_date = datetime(2009, 12, 31)
    correct_next_date = datetime(2010, 1, 6)
    eq_(trigger.get_next_fire_time(start_date), correct_next_date)

@raises(ValueError)
def test_cron_faulty_expr():
    CronTrigger(year='2009-fault')

def test_date_trigger_earlier():
    fire_date = datetime(2009, 7, 6)
    trigger = DateTrigger(fire_date)
    start_date = datetime(2008, 12, 1)
    eq_(trigger.get_next_fire_time(start_date), fire_date)

def test_date_trigger_exact():
    fire_date = datetime(2009, 7, 6)
    trigger = DateTrigger(fire_date)
    start_date = datetime(2009, 7, 6)
    eq_(trigger.get_next_fire_time(start_date), fire_date)

def test_date_trigger_later():
    fire_date = datetime(2009, 7, 6)
    trigger = DateTrigger(fire_date)
    start_date = datetime(2009, 7, 7)
    eq_(trigger.get_next_fire_time(start_date), None)

@raises(TypeError)
def test_interval_invalid_interval():
    IntervalTrigger('1-6', 3)

@raises(ValueError)
def test_interval_invalid_repeat():
    interval = timedelta(seconds=1)
    IntervalTrigger(interval, -1)

def test_interval_infinite():
    interval = timedelta(seconds=1)
    trigger = IntervalTrigger(interval, 0)
    eq_(trigger.last_fire_date, None)

class TestInterval(object):
    def setUp(self):
        interval = timedelta(seconds=1)
        trigger_start_date = datetime(2009, 8, 4, second=2)
        self.trigger = IntervalTrigger(interval, 3, trigger_start_date)
    
    def test_interval_before(self):
        start_date = datetime(2009, 8, 4)
        correct_next_date = datetime(2009, 8, 4, second=2)
        eq_(self.trigger.get_next_fire_time(start_date), correct_next_date)

    def test_interval_within(self):
        start_date = datetime(2009, 8, 4, second=2, microsecond=1000)
        correct_next_date = datetime(2009, 8, 4, second=3)
        eq_(self.trigger.get_next_fire_time(start_date), correct_next_date)

    def test_interval_after(self):
        start_date = datetime(2009, 8, 4, second=4, microsecond=1000)
        eq_(self.trigger.get_next_fire_time(start_date), None)
