# coding: utf-8
from datetime import date, datetime, timedelta
import time
import os
import shelve

from dateutil.tz import tzoffset
import pytest

from apscheduler.util import *
from tests.conftest import minpython


local_tz = tzoffset('DUMMYTZ', 3600)


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


def test_asint_invalid_1():
    pytest.raises(ValueError, asint, '5s')


def test_asint_invalid_2():
    pytest.raises(ValueError, asint, 'shplse')


def test_asint_number():
    assert asint('539') == 539


def test_asint_none():
    assert asint(None) is None


def test_asbool_true():
    for val in (' True', 'true ', 'Yes', ' yes ', '1  '):
        assert asbool(val) is True
    assert asbool(True) is True


def test_asbool_false():
    for val in (' False', 'false ', 'No', ' no ', '0  '):
        assert asbool(val) is False
    assert asbool(False) is False


def test_asbool_fail():
    pytest.raises(ValueError, asbool, 'yep')


def test_convert_datetime_date():
    dateval = date(2009, 8, 1)
    datetimeval = convert_to_datetime(dateval, local_tz, None)
    correct_datetime = datetime(2009, 8, 1)
    assert isinstance(datetimeval, datetime)
    assert datetimeval == correct_datetime.replace(tzinfo=local_tz)


def test_convert_datetime_passthrough():
    datetimeval = datetime(2009, 8, 1, 5, 6, 12)
    convertedval = convert_to_datetime(datetimeval, local_tz, None)
    assert convertedval == datetimeval.replace(tzinfo=local_tz)


def test_convert_datetime_text1():
    convertedval = convert_to_datetime('2009-8-1', local_tz, None)
    assert convertedval == datetime(2009, 8, 1, tzinfo=local_tz)


def test_convert_datetime_text2():
    convertedval = convert_to_datetime('2009-8-1 5:16:12', local_tz, None)
    assert convertedval == datetime(2009, 8, 1, 5, 16, 12, tzinfo=local_tz)


def test_datestring_parse_datetime_micro():
    convertedval = convert_to_datetime('2009-8-1 5:16:12.843821', local_tz, None)
    assert convertedval == datetime(2009, 8, 1, 5, 16, 12, 843821, tzinfo=local_tz)


def test_existing_tzinfo():
    alter_tz = tzoffset('ALTERNATE', -3600)
    dateval = datetime(2009, 8, 1, tzinfo=alter_tz)
    assert convert_to_datetime(dateval, local_tz, None) == dateval


def test_convert_datetime_invalid():
    pytest.raises(TypeError, convert_to_datetime, 995302092123, local_tz, None)


def test_convert_datetime_invalid_str():
    pytest.raises(ValueError, convert_to_datetime, '19700-12-1', local_tz, None)


def test_timedelta_seconds():
    delta = timedelta(minutes=2, seconds=30)
    seconds = timedelta_seconds(delta)
    assert seconds == 150


def test_time_difference_positive():
    earlier = datetime(2008, 9, 1, second=3)
    later = datetime(2008, 9, 1, second=49)
    assert time_difference(later, earlier) == 46


def test_time_difference_negative():
    earlier = datetime(2009, 4, 7, second=7)
    later = datetime(2009, 4, 7, second=56)
    assert time_difference(earlier, later) == -49


class TestDSTTimeDifference(object):
    @pytest.fixture(scope='class', autouse=True)
    def timezone(self, request):
        def finish():
            del os.environ['TZ']
            time.tzset()

        if hasattr(time, 'tzset'):
            os.environ['TZ'] = 'Europe/Helsinki'
            time.tzset()
            request.addfinalizer(finish)

    def test_time_difference_daylight_1(self):
        earlier = datetime(2010, 3, 28, 2)
        later = datetime(2010, 3, 28, 4)
        assert time_difference(later, earlier) == 3600

    def test_time_difference_daylight_2(self):
        earlier = datetime(2010, 10, 31, 2)
        later = datetime(2010, 10, 31, 5)
        assert time_difference(later, earlier) == 14400


def test_datetime_ceil_round():
    dateval = datetime(2009, 4, 7, 2, 10, 16, 4000)
    correct_answer = datetime(2009, 4, 7, 2, 10, 17)
    assert datetime_ceil(dateval) == correct_answer


def test_datetime_ceil_exact():
    dateval = datetime(2009, 4, 7, 2, 10, 16)
    correct_answer = datetime(2009, 4, 7, 2, 10, 16)
    assert datetime_ceil(dateval) == correct_answer


def test_combine_opts():
    global_opts = {'someprefix.opt1': '123',
                   'opt2': '456',
                   'someprefix.opt3': '789'}
    local_opts = {'opt3': 'abc'}
    combined = combine_opts(global_opts, 'someprefix.', local_opts)
    assert combined == dict(opt1='123', opt3='abc')


def test_callable_name():
    assert get_callable_name(test_callable_name) == 'test_callable_name'
    assert get_callable_name(DummyClass.staticmeth) == 'staticmeth'
    assert get_callable_name(DummyClass.classmeth) == 'DummyClass.classmeth'
    assert get_callable_name(DummyClass.meth) == 'meth'
    assert get_callable_name(DummyClass().meth) == 'DummyClass.meth'
    assert get_callable_name(DummyClass) == 'DummyClass'
    assert get_callable_name(DummyClass()) == 'DummyClass'
    pytest.raises(TypeError, get_callable_name, object())


def test_obj_to_ref():
    pytest.raises(ValueError, obj_to_ref, DummyClass.meth)
    pytest.raises(ValueError, obj_to_ref, DummyClass.staticmeth)
    assert obj_to_ref(DummyClass.classmeth) == 'tests.test_util:DummyClass.classmeth'
    assert obj_to_ref(shelve.open) == 'shelve:open'


@minpython(3, 3)
def test_inner_obj_to_ref():
    assert obj_to_ref(DummyClass.InnerDummyClass.innerclassmeth) == \
        'tests.test_util:DummyClass.InnerDummyClass.innerclassmeth'


def test_ref_to_obj():
    assert ref_to_obj('shelve:open') == shelve.open
    pytest.raises(TypeError, ref_to_obj, object())
    pytest.raises(ValueError, ref_to_obj, 'module')
    pytest.raises(LookupError, ref_to_obj, 'module:blah')


def test_maybe_ref():
    assert maybe_ref('shelve:open') == shelve.open
    assert maybe_ref(shelve.open) == shelve.open
