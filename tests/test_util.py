# coding: utf-8
from datetime import date, datetime, timedelta, tzinfo
from functools import partial

import pytest
import pytz
import six
import sys

from apscheduler.util import (
    asint, asbool, astimezone, convert_to_datetime, datetime_to_utc_timestamp, utc_timestamp_to_datetime,
    timedelta_seconds, datetime_ceil, get_callable_name, obj_to_ref, ref_to_obj, maybe_ref, check_callable_args,
    datetime_repr, repr_escape)
from tests.conftest import minpython, maxpython

try:
    from unittest.mock import Mock
except ImportError:
    from mock import Mock


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


class TestAsint(object):
    @pytest.mark.parametrize('value', ['5s', 'shplse'], ids=['digit first', 'text'])
    def test_invalid_value(self, value):
        pytest.raises(ValueError, asint, value)

    def test_number(self):
        assert asint('539') == 539

    def test_none(self):
        assert asint(None) is None


class TestAsbool(object):
    @pytest.mark.parametrize('value', [' True', 'true ', 'Yes', ' yes ', '1  ', True],
                             ids=['capital true', 'lowercase true', 'capital yes', 'lowercase yes', 'one', 'True'])
    def test_true(self, value):
        assert asbool(value) is True

    @pytest.mark.parametrize('value', [' False', 'false ', 'No', ' no ', '0  ', False],
                             ids=['capital', 'lowercase false', 'capital no', 'lowercase no', 'zero', 'False'])
    def test_false(self, value):
        assert asbool(value) is False

    def test_bad_value(self):
        pytest.raises(ValueError, asbool, 'yep')


class TestAstimezone(object):
    def test_str(self):
        value = astimezone('Europe/Helsinki')
        assert isinstance(value, tzinfo)

    def test_tz(self):
        tz = pytz.timezone('Europe/Helsinki')
        value = astimezone(tz)
        assert tz is value

    def test_none(self):
        assert astimezone(None) is None

    def test_bad_timezone_type(self):
        exc = pytest.raises(TypeError, astimezone, tzinfo())
        assert 'Only timezones from the pytz library are supported' in str(exc.value)

    def test_bad_local_timezone(self):
        zone = Mock(tzinfo, localize=None, normalize=None, zone='local')
        exc = pytest.raises(ValueError, astimezone, zone)
        assert 'Unable to determine the name of the local timezone' in str(exc.value)

    def test_bad_value(self):
        exc = pytest.raises(TypeError, astimezone, 4)
        assert 'Expected tzinfo, got int instead' in str(exc.value)


class TestConvertToDatetime(object):
    @pytest.mark.parametrize('input,expected', [
        (None, None),
        (date(2009, 8, 1), datetime(2009, 8, 1)),
        (datetime(2009, 8, 1, 5, 6, 12), datetime(2009, 8, 1, 5, 6, 12)),
        ('2009-8-1', datetime(2009, 8, 1)),
        ('2009-8-1 5:16:12', datetime(2009, 8, 1, 5, 16, 12)),
        (pytz.FixedOffset(-60).localize(datetime(2009, 8, 1)), pytz.FixedOffset(-60).localize(datetime(2009, 8, 1)))
    ], ids=['None', 'date', 'datetime', 'date as text', 'datetime as text', 'existing tzinfo'])
    def test_date(self, timezone, input, expected):
        returned = convert_to_datetime(input, timezone, None)
        if expected is not None:
            assert isinstance(returned, datetime)
            expected = timezone.localize(expected) if not expected.tzinfo else expected

        assert returned == expected

    def test_invalid_input_type(self, timezone):
        exc = pytest.raises(TypeError, convert_to_datetime, 92123, timezone, 'foo')
        assert str(exc.value) == 'Unsupported type for foo: int'

    def test_invalid_input_value(self, timezone):
        exc = pytest.raises(ValueError, convert_to_datetime, '19700-12-1', timezone, None)
        assert str(exc.value) == 'Invalid date string'

    def test_missing_timezone(self):
        exc = pytest.raises(ValueError, convert_to_datetime, '2009-8-1', None, 'argname')
        assert str(exc.value) == 'The "tz" argument must be specified if argname has no timezone information'

    def test_text_timezone(self):
        returned = convert_to_datetime('2009-8-1', 'UTC', None)
        assert returned == datetime(2009, 8, 1, tzinfo=pytz.utc)

    def test_bad_timezone(self):
        exc = pytest.raises(TypeError, convert_to_datetime, '2009-8-1', tzinfo(), None)
        assert str(exc.value) == 'Only pytz timezones are supported (need the localize() and normalize() methods)'


def test_datetime_to_utc_timestamp(timezone):
    dt = timezone.localize(datetime(2014, 3, 12, 5, 40, 13, 254012))
    timestamp = datetime_to_utc_timestamp(dt)
    dt2 = utc_timestamp_to_datetime(timestamp)
    assert dt2 == dt


def test_timedelta_seconds():
    delta = timedelta(minutes=2, seconds=30)
    seconds = timedelta_seconds(delta)
    assert seconds == 150


@pytest.mark.parametrize('input,expected', [
    (datetime(2009, 4, 7, 2, 10, 16, 4000), datetime(2009, 4, 7, 2, 10, 17)),
    (datetime(2009, 4, 7, 2, 10, 16), datetime(2009, 4, 7, 2, 10, 16))
], ids=['milliseconds', 'exact'])
def test_datetime_ceil(input, expected):
    assert datetime_ceil(input) == expected


@pytest.mark.parametrize('input,expected', [
    (None, 'None'),
    (pytz.timezone('Europe/Helsinki').localize(datetime(2014, 5, 30, 7, 12, 20)), '2014-05-30 07:12:20 EEST')
], ids=['None', 'datetime+tzinfo'])
def test_datetime_repr(input, expected):
    assert datetime_repr(input) == expected


class TestGetCallableName(object):
    @pytest.mark.parametrize('input,expected', [
        (asint, 'asint'),
        (DummyClass.staticmeth, 'DummyClass.staticmeth' if hasattr(DummyClass, '__qualname__') else 'staticmeth'),
        (DummyClass.classmeth, 'DummyClass.classmeth'),
        (DummyClass.meth, 'meth' if sys.version_info[:2] == (3, 2) else 'DummyClass.meth'),
        (DummyClass().meth, 'DummyClass.meth'),
        (DummyClass, 'DummyClass'),
        (DummyClass(), 'DummyClass')
    ], ids=['function', 'static method', 'class method', 'unbounded method', 'bounded method', 'class', 'instance'])
    def test_inputs(self, input, expected):
        assert get_callable_name(input) == expected

    def test_bad_input(self):
        pytest.raises(TypeError, get_callable_name, object())


class TestObjToRef(object):
    @pytest.mark.parametrize('input', [partial(DummyClass.meth)], ids=['partial/bound method'])
    def test_no_ref_found(self, input):
        exc = pytest.raises(ValueError, obj_to_ref, input)
        assert 'Cannot determine the reference to ' in str(exc.value)

    @pytest.mark.parametrize('input,expected', [
        pytest.mark.skipif(sys.version_info[:2] == (3, 2), reason="Unbound methods can't be resolved on Python 3.2")(
            (DummyClass.meth, 'tests.test_util:DummyClass.meth')
        ),
        (DummyClass.classmeth, 'tests.test_util:DummyClass.classmeth'),
        pytest.mark.skipif(sys.version_info < (3, 3), reason="Requires __qualname__ (Python 3.3+)")(
            (DummyClass.InnerDummyClass.innerclassmeth, 'tests.test_util:DummyClass.InnerDummyClass.innerclassmeth')
        ),
        pytest.mark.skipif(sys.version_info < (3, 3), reason="Requires __qualname__ (Python 3.3+)")(
            (DummyClass.staticmeth, 'tests.test_util:DummyClass.staticmeth')
        ),
        (timedelta, 'datetime:timedelta'),
    ], ids=['unbound method', 'class method', 'inner class method', 'static method', 'timedelta'])
    def test_valid_refs(self, input, expected):
        assert obj_to_ref(input) == expected


class TestRefToObj(object):
    def test_valid_ref(self):
        from logging.handlers import RotatingFileHandler
        assert ref_to_obj('logging.handlers:RotatingFileHandler') is RotatingFileHandler

    @pytest.mark.parametrize('input,error', [
        (object(), TypeError),
        ('module', ValueError),
        ('module:blah', LookupError)
    ], ids=['raw object', 'module', 'module attribute'])
    def test_lookup_error(self, input, error):
        pytest.raises(error, ref_to_obj, input)


@pytest.mark.parametrize('input,expected', [
    ('datetime:timedelta', timedelta),
    (timedelta, timedelta)
], ids=['textref', 'direct'])
def test_maybe_ref(input, expected):
    assert maybe_ref(input) == expected


@pytest.mark.parametrize('input,expected', [
    (b'T\xc3\xa9st'.decode('utf-8'), 'T\\xe9st' if six.PY2 else 'Tést'),
    (1, 1)
], ids=['string', 'int'])
@maxpython(3)
def test_repr_escape_py2(input, expected):
    assert repr_escape(input) == expected


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

    def test_default_args(self, use_signature):
        """Tests that default values for arguments are properly taken into account."""

        exc = pytest.raises(ValueError, check_callable_args, lambda x, y, z=1: None, [1], {})
        assert str(exc.value) == 'The following arguments have not been supplied: y'

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
