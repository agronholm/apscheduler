from datetime import date, datetime, timedelta

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
    dateval = datetime(2008, 2, 1)
    eq_(get_actual_maximum(dateval, 'day'), 29)

def test_maxval_nonleap():
    dateval = datetime(2009, 2, 1)
    eq_(get_actual_maximum(dateval, 'day'), 28)

def test_date_field():
    dateval = datetime(2008, 7, 9, 10, 0, 3)
    eq_(get_date_field(dateval, 'year'), 2008)
    eq_(get_date_field(dateval, 'month'), 7)
    eq_(get_date_field(dateval, 'day'), 9)
    eq_(get_date_field(dateval, 'day_of_week'), 2)
    eq_(get_date_field(dateval, 'hour'), 10)
    eq_(get_date_field(dateval, 'minute'), 0)
    eq_(get_date_field(dateval, 'second'), 3)

def test_convert_datetime_date():
    dateval = date(2009, 8, 1)
    datetimeval = convert_to_datetime(dateval)
    correct_datetime = datetime(2009, 8, 1)
    assert isinstance(datetimeval, datetime)
    eq_(datetimeval, correct_datetime)

def test_convert_datetime_passthrough():
    datetimeval = datetime(2009, 8, 1, 5, 6, 12)
    convertedval = convert_to_datetime(datetimeval)
    eq_(convertedval, datetimeval)

@raises(TypeError)
def test_convert_datetime_invalid():
    convert_to_datetime('2009-4-5')

def test_timedelta_seconds():
    delta = timedelta(minutes=2, seconds=30)
    seconds = timedelta_seconds(delta)
    eq_(seconds, 150)

def test_time_difference_positive():
    earlier = datetime(2008, 9, 1, second=3)
    later = datetime(2008, 9, 1, second=49)
    eq_(time_difference(later, earlier), 46)

def test_time_difference_negative():
    earlier = datetime(2009, 4, 7, second=7)
    later = datetime(2009, 4, 7, second=56)
    eq_(time_difference(earlier, later), -49)

def test_datetime_ceil_round():
    dateval = datetime(2009, 4, 7, 2, 10, 16, 4000)
    correct_answer = datetime(2009, 4, 7, 2, 10, 17)
    eq_(datetime_ceil(dateval), correct_answer)

def test_datetime_ceil_exact():
    dateval = datetime(2009, 4, 7, 2, 10, 16)
    correct_answer = datetime(2009, 4, 7, 2, 10, 16)
    eq_(datetime_ceil(dateval), correct_answer)
