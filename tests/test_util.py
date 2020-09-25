import platform
import sys
from datetime import datetime, timedelta
from functools import partial
from types import ModuleType

import pytest

from apscheduler.util import (
    datetime_ceil, get_callable_name, obj_to_ref, ref_to_obj, maybe_ref, check_callable_args)


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


class InheritedDummyClass(DummyClass):
    @classmethod
    def classmeth(cls):
        pass


@pytest.mark.parametrize('input,expected', [
    (datetime(2009, 4, 7, 2, 10, 16, 4000), datetime(2009, 4, 7, 2, 10, 17)),
    (datetime(2009, 4, 7, 2, 10, 16), datetime(2009, 4, 7, 2, 10, 16))
], ids=['milliseconds', 'exact'])
def test_datetime_ceil(input, expected):
    assert datetime_ceil(input) == expected


class TestGetCallableName(object):
    @pytest.mark.parametrize('input,expected', [
        (open, 'open'),
        (DummyClass.staticmeth, 'DummyClass.staticmeth' if
         hasattr(DummyClass, '__qualname__') else 'staticmeth'),
        (DummyClass.classmeth, 'DummyClass.classmeth'),
        (DummyClass.meth, 'DummyClass.meth'),
        (DummyClass().meth, 'DummyClass.meth'),
        (DummyClass, 'DummyClass'),
        (DummyClass(), 'DummyClass')
    ], ids=['function', 'static method', 'class method', 'unbounded method', 'bounded method',
            'class', 'instance'])
    def test_inputs(self, input, expected):
        assert get_callable_name(input) == expected

    def test_bad_input(self):
        pytest.raises(TypeError, get_callable_name, object())


class TestObjToRef(object):
    @pytest.mark.parametrize('obj, error', [
        (partial(DummyClass.meth), 'Cannot create a reference to a partial()'),
        (lambda: None, 'Cannot create a reference to a lambda')
    ], ids=['partial', 'lambda'])
    def test_errors(self, obj, error):
        exc = pytest.raises(ValueError, obj_to_ref, obj)
        assert str(exc.value) == error

    def test_nested_function_error(self):
        def nested():
            pass

        exc = pytest.raises(ValueError, obj_to_ref, nested)
        assert str(exc.value) == 'Cannot create a reference to a nested function'

    @pytest.mark.parametrize('input,expected', [
        (DummyClass.meth, 'test_util:DummyClass.meth'),
        (DummyClass.classmeth, 'test_util:DummyClass.classmeth'),
        (DummyClass.InnerDummyClass.innerclassmeth,
         'test_util:DummyClass.InnerDummyClass.innerclassmeth'),
        (DummyClass.staticmeth, 'test_util:DummyClass.staticmeth'),
        (InheritedDummyClass.classmeth, 'test_util:InheritedDummyClass.classmeth'),
        (timedelta, 'datetime:timedelta'),
    ], ids=['unbound method', 'class method', 'inner class method', 'static method',
            'inherited class method', 'timedelta'])
    def test_valid_refs(self, input, expected):
        assert obj_to_ref(input) == expected


class TestRefToObj(object):
    def test_valid_ref(self):
        from logging.handlers import RotatingFileHandler
        assert ref_to_obj('logging.handlers:RotatingFileHandler') is RotatingFileHandler

    def test_complex_path(self):
        pkg1 = ModuleType('pkg1')
        pkg1.pkg2 = 'blah'
        pkg2 = ModuleType('pkg1.pkg2')
        pkg2.varname = 'test'
        sys.modules['pkg1'] = pkg1
        sys.modules['pkg1.pkg2'] = pkg2
        assert ref_to_obj('pkg1.pkg2:varname') == 'test'

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


class TestCheckCallableArgs(object):
    def test_invalid_callable_args(self):
        """
        Tests that attempting to create a job with an invalid number of arguments raises an
        exception.

        """
        exc = pytest.raises(ValueError, check_callable_args, lambda x: None, [1, 2], {})
        assert str(exc.value) == (
            'The list of positional arguments is longer than the target callable can handle '
            '(allowed: 1, given in args: 2)')

    def test_invalid_callable_kwargs(self):
        """
        Tests that attempting to schedule a job with unmatched keyword arguments raises an
        exception.

        """
        exc = pytest.raises(ValueError, check_callable_args, lambda x: None, [], {'x': 0, 'y': 1})
        assert str(exc.value) == ('The target callable does not accept the following keyword '
                                  'arguments: y')

    def test_missing_callable_args(self):
        """Tests that attempting to schedule a job with missing arguments raises an exception."""
        exc = pytest.raises(ValueError, check_callable_args, lambda x, y, z: None, [1], {'y': 0})
        assert str(exc.value) == 'The following arguments have not been supplied: z'

    def test_default_args(self):
        """Tests that default values for arguments are properly taken into account."""
        exc = pytest.raises(ValueError, check_callable_args, lambda x, y, z=1: None, [1], {})
        assert str(exc.value) == 'The following arguments have not been supplied: y'

    def test_conflicting_callable_args(self):
        """
        Tests that attempting to schedule a job where the combination of args and kwargs are in
        conflict raises an exception.

        """
        exc = pytest.raises(ValueError, check_callable_args, lambda x, y: None, [1, 2], {'y': 1})
        assert str(exc.value) == 'The following arguments are supplied in both args and kwargs: y'

    def test_signature_positional_only(self):
        """Tests that a function where signature() fails is accepted."""
        check_callable_args(object().__setattr__, ('blah', 1), {})

    @pytest.mark.skipif(platform.python_implementation() == 'PyPy',
                        reason='PyPy does not expose signatures of builtins')
    def test_positional_only_args(self):
        """
        Tests that an attempt to use keyword arguments for positional-only arguments raises an
        exception.

        """
        exc = pytest.raises(ValueError, check_callable_args, object.__setattr__, ['blah'],
                            {'value': 1})
        assert str(exc.value) == ('The following arguments cannot be given as keyword arguments: '
                                  'value')

    def test_unfulfilled_kwargs(self):
        """
        Tests that attempting to schedule a job where not all keyword-only arguments are fulfilled
        raises an exception.

        """
        func = eval("lambda x, *, y, z=1: None")
        exc = pytest.raises(ValueError, check_callable_args, func, [1], {})
        assert str(exc.value) == ('The following keyword-only arguments have not been supplied in '
                                  'kwargs: y')
