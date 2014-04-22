# coding: utf-8
from datetime import date, datetime, timedelta, tzinfo
from functools import partial
import shelve

from dateutil.tz import tzoffset, gettz
import pytest

from apscheduler.util import (
    asint, asbool, astimezone, convert_to_datetime, datetime_to_utc_timestamp, utc_timestamp_to_datetime,
    timedelta_seconds, datetime_ceil, combine_opts, get_callable_name, obj_to_ref, ref_to_obj, maybe_ref,
    check_callable_args)
from tests.conftest import minpython


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


def test_astimezone_str():
    value = astimezone('Europe/Helsinki')
    assert isinstance(value, tzinfo)


def test_astimezone_tz():
    tz = gettz('Europe/Helsinki')
    value = astimezone(tz)
    assert tz is value


def test_astimezone_none():
    assert astimezone(None) is None


def test_astimezone_fail():
    pytest.raises(TypeError, astimezone, 4)


def test_convert_datetime_date(timezone):
    dateval = date(2009, 8, 1)
    datetimeval = convert_to_datetime(dateval, timezone, None)
    correct_datetime = datetime(2009, 8, 1)
    assert isinstance(datetimeval, datetime)
    assert datetimeval == correct_datetime.replace(tzinfo=timezone)


def test_convert_datetime_passthrough(timezone):
    datetimeval = datetime(2009, 8, 1, 5, 6, 12)
    convertedval = convert_to_datetime(datetimeval, timezone, None)
    assert convertedval == datetimeval.replace(tzinfo=timezone)


def test_convert_datetime_text1(timezone):
    convertedval = convert_to_datetime('2009-8-1', timezone, None)
    assert convertedval == datetime(2009, 8, 1, tzinfo=timezone)


def test_convert_datetime_text2(timezone):
    convertedval = convert_to_datetime('2009-8-1 5:16:12', timezone, None)
    assert convertedval == datetime(2009, 8, 1, 5, 16, 12, tzinfo=timezone)


def test_datestring_parse_datetime_micro(timezone):
    convertedval = convert_to_datetime('2009-8-1 5:16:12.843821', timezone, None)
    assert convertedval == datetime(2009, 8, 1, 5, 16, 12, 843821, tzinfo=timezone)


def test_existing_tzinfo(timezone):
    alter_tz = tzoffset('ALTERNATE', -3600)
    dateval = datetime(2009, 8, 1, tzinfo=alter_tz)
    assert convert_to_datetime(dateval, timezone, None) == dateval


def test_convert_datetime_invalid(timezone):
    pytest.raises(TypeError, convert_to_datetime, 995302092123, timezone, None)


def test_convert_datetime_invalid_str(timezone):
    pytest.raises(ValueError, convert_to_datetime, '19700-12-1', timezone, None)


def test_datetime_to_utc_timestamp(timezone):
    dt = datetime(2014, 3, 12, 5, 40, 13, 254012, tzinfo=timezone)
    timestamp = datetime_to_utc_timestamp(dt)
    dt2 = utc_timestamp_to_datetime(timestamp)
    assert dt2 == dt


def test_timedelta_seconds():
    delta = timedelta(minutes=2, seconds=30)
    seconds = timedelta_seconds(delta)
    assert seconds == 150


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
    pytest.raises(ValueError, obj_to_ref, partial(DummyClass.meth))
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


class TestCheckCallableArgs(object):
    @pytest.fixture(params=[True, False], ids=['signature', 'getargspec'])
    def use_signature(self, request, monkeypatch):
        if not request.param:
            monkeypatch.setattr('apscheduler.util.signature', None)

    def test_invalid_callable_args(self, use_signature):
        """Tests that attempting to create a job with an invalid number of arguments raises an exception."""

        exc = pytest.raises(ValueError, check_callable_args, lambda x: None, [1, 2], {})
        assert str(exc.value) == ('The list of positional arguments is longer than the target callable can handle '
                                  '(allowed: 1, given in args: 2)')

    def test_invalid_callable_kwargs(self, use_signature):
        """Tests that attempting to schedule a job with unmatched keyword arguments raises an exception."""

        exc = pytest.raises(ValueError, check_callable_args, lambda x: None, [], {'x': 0, 'y': 1})
        assert str(exc.value) == 'The target callable does not accept the following keyword arguments: y'

    def test_missing_callable_args(self, use_signature):
        """Tests that attempting to schedule a job with missing arguments raises an exception."""

        exc = pytest.raises(ValueError, check_callable_args, lambda x, y, z: None, [1], {'y': 0})
        assert str(exc.value) == 'The following arguments have not been supplied: z'

    def test_conflicting_callable_args(self, use_signature):
        """
        Tests that attempting to schedule a job where the combination of args and kwargs are in conflict raises an
        exception.
        """

        exc = pytest.raises(ValueError, check_callable_args, lambda x, y: None, [1, 2], {'y': 1})
        assert str(exc.value) == 'The following arguments are supplied in both args and kwargs: y'

    def test_signature_positional_only(self):
        """Tests that a function where signature() fails is accepted."""

        check_callable_args(object().__setattr__, ('blah', 1), {})

    @minpython(3, 4)
    def test_positional_only_args(self):
        """Tests that an attempt to use keyword arguments for positional-only arguments raises an exception."""

        exc = pytest.raises(ValueError, check_callable_args, object.__setattr__, ['blah'], {'value': 1})
        assert str(exc.value) == 'The following arguments cannot be given as keyword arguments: value'

    @minpython(3)
    def test_unfulfilled_kwargs(self):
        """
        Tests that attempting to schedule a job where not all keyword-only arguments are fulfilled raises an
        exception.
        """

        func = eval("lambda x, *, y, z=1: None")
        exc = pytest.raises(ValueError, check_callable_args, func, [1], {})
        assert str(exc.value) == 'The following keyword-only arguments have not been supplied in kwargs: y'
