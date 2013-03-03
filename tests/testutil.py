# coding: utf-8
from datetime import date, datetime, timedelta
import time
import os
import sys
import shelve

from nose.tools import eq_, raises, assert_raises  # @UnresolvedImport
from nose.plugins.skip import SkipTest

from apscheduler.util import *


class DummyClass(object):
    def meth(self):
        pass

    @staticmethod
    def staticmeth():
        pass

    @classmethod
    def classmeth(cls):
        pass

    def __call__(self):
        pass

    class InnerDummyClass(object):
        @classmethod
        def innerclassmeth(cls):
            pass


def meth():
    pass


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


def test_asbool_true():
    for val in (' True', 'true ', 'Yes', ' yes ', '1  '):
        eq_(asbool(val), True)
    assert asbool(True) is True


def test_asbool_false():
    for val in (' False', 'false ', 'No', ' no ', '0  '):
        eq_(asbool(val), False)
    assert asbool(False) is False


@raises(ValueError)
def test_asbool_fail():
    asbool('yep')


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


def test_convert_datetime_text1():
    convertedval = convert_to_datetime('2009-8-1')
    eq_(convertedval, datetime(2009, 8, 1))


def test_convert_datetime_text2():
    convertedval = convert_to_datetime('2009-8-1 5:16:12')
    eq_(convertedval, datetime(2009, 8, 1, 5, 16, 12))


def test_datestring_parse_datetime_micro():
    convertedval = convert_to_datetime('2009-8-1 5:16:12.843821')
    eq_(convertedval, datetime(2009, 8, 1, 5, 16, 12, 843821))


@raises(TypeError)
def test_convert_datetime_invalid():
    convert_to_datetime(995302092123)


@raises(ValueError)
def test_convert_datetime_invalid_str():
    convert_to_datetime('19700-12-1')


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


class TestDSTTimeDifference(object):
    def setup(self):
        if hasattr(time, 'tzset'):
            os.environ['TZ'] = 'Europe/Helsinki'
            time.tzset()

    def teardown(self):
        if hasattr(time, 'tzset'):
            del os.environ['TZ']
            time.tzset()

    def test_time_difference_daylight_1(self):
        earlier = datetime(2010, 3, 28, 2)
        later = datetime(2010, 3, 28, 4)
        eq_(time_difference(later, earlier), 3600)

    def test_time_difference_daylight_2(self):
        earlier = datetime(2010, 10, 31, 2)
        later = datetime(2010, 10, 31, 5)
        eq_(time_difference(later, earlier), 14400)


def test_datetime_ceil_round():
    dateval = datetime(2009, 4, 7, 2, 10, 16, 4000)
    correct_answer = datetime(2009, 4, 7, 2, 10, 17)
    eq_(datetime_ceil(dateval), correct_answer)


def test_datetime_ceil_exact():
    dateval = datetime(2009, 4, 7, 2, 10, 16)
    correct_answer = datetime(2009, 4, 7, 2, 10, 16)
    eq_(datetime_ceil(dateval), correct_answer)


def test_combine_opts():
    global_opts = {'someprefix.opt1': '123',
                   'opt2': '456',
                   'someprefix.opt3': '789'}
    local_opts = {'opt3': 'abc'}
    combined = combine_opts(global_opts, 'someprefix.', local_opts)
    eq_(combined, dict(opt1='123', opt3='abc'))


def test_callable_name():
    eq_(get_callable_name(test_callable_name), 'test_callable_name')
    eq_(get_callable_name(DummyClass.staticmeth), 'staticmeth')
    eq_(get_callable_name(DummyClass.classmeth), 'DummyClass.classmeth')
    eq_(get_callable_name(DummyClass.meth), 'meth')
    eq_(get_callable_name(DummyClass().meth), 'DummyClass.meth')
    eq_(get_callable_name(DummyClass), 'DummyClass')
    eq_(get_callable_name(DummyClass()), 'DummyClass')
    assert_raises(TypeError, get_callable_name, object())


def test_obj_to_ref():
    assert_raises(ValueError, obj_to_ref, DummyClass.meth)
    assert_raises(ValueError, obj_to_ref, DummyClass.staticmeth)
    eq_(obj_to_ref(DummyClass.classmeth), 'testutil:DummyClass.classmeth')
    eq_(obj_to_ref(shelve.open), 'shelve:open')


def test_inner_obj_to_ref():
    if sys.version_info < (3, 3):
        raise SkipTest
    eq_(obj_to_ref(DummyClass.InnerDummyClass.innerclassmeth), 'testutil:DummyClass.InnerDummyClass.innerclassmeth')


def test_ref_to_obj():
    eq_(ref_to_obj('shelve:open'), shelve.open)
    assert_raises(TypeError, ref_to_obj, object())
    assert_raises(ValueError, ref_to_obj, 'module')
    assert_raises(LookupError, ref_to_obj, 'module:blah')


def test_maybe_ref():
    eq_(maybe_ref('shelve:open'), shelve.open)
    eq_(maybe_ref(shelve.open), shelve.open)


def test_to_unicode():
    if sys.version_info[0] < 3:
        eq_(to_unicode('aaööbb'), unicode('aabb'))
        eq_(to_unicode(unicode('gfkj')), unicode('gfkj'))
    else:
        eq_(to_unicode('aaööbb'.encode('utf-8')), 'aabb')
        eq_(to_unicode('gfkj'), 'gfkj')
