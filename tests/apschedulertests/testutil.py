from datetime import datetime, timedelta

from nose.tools import eq_, raises

from apscheduler.util import *


@raises(ValueError)
def test_asint_invalid_1():
    asint('5s')

@raises(ValueError)
def test_asint_invalid_2():
    asint('shplse')

def test_asint_number():
    eq_(asint('539'), 539)

def test_asint_none():
    eq_(asint(None), None)
    
def test_maxval_leapyear():
    date = datetime(2008, 2, 1)
    eq_(get_actual_maximum(date, 'day'), 29)

def test_maxval_nonleap():
    date = datetime(2009, 2, 1)
    eq_(get_actual_maximum(date, 'day'), 28)

def test_date_field():
    date = datetime(2008, 7, 9, 10, 0, 3)
    eq_(get_date_field(date, 'year'), 2008)
    eq_(get_date_field(date, 'month'), 7)
    eq_(get_date_field(date, 'day'), 9)
    eq_(get_date_field(date, 'day_of_week'), 2)
    eq_(get_date_field(date, 'hour'), 10)
    eq_(get_date_field(date, 'minute'), 0)
    eq_(get_date_field(date, 'second'), 3)

def test_timedelta_seconds():
    delta = timedelta()

def test_time_difference_positive():
    earlier = datetime(2008, 9, 1, second=3)
    later = datetime(2008, 9, 1, second=49)
    eq_(time_difference(later, earlier), 46)

def test_time_difference_negative():
    earlier = datetime(2009, 4, 7, second=7)
    later = datetime(2009, 4, 7, second=56)
    eq_(time_difference(earlier, later), -49)
