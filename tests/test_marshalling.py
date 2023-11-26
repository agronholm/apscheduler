from __future__ import annotations

import sys
from datetime import timedelta
from functools import partial
from types import ModuleType

import pytest

from apscheduler import SerializationError
from apscheduler._marshalling import callable_from_ref, callable_to_ref


class DummyClass:
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

    class InnerDummyClass:
        @classmethod
        def innerclassmeth(cls):
            pass


class InheritedDummyClass(DummyClass):
    pass


class TestCallableToRef:
    @pytest.mark.parametrize(
        "obj, error",
        [
            (partial(DummyClass.meth), "Cannot create a reference to a partial()"),
            (lambda: None, "Cannot create a reference to a lambda"),
        ],
        ids=["partial", "lambda"],
    )
    def test_errors(self, obj, error):
        exc = pytest.raises(SerializationError, callable_to_ref, obj)
        assert str(exc.value) == error

    def test_nested_function_error(self):
        def nested():
            pass

        exc = pytest.raises(SerializationError, callable_to_ref, nested)
        assert str(exc.value) == "Cannot create a reference to a nested function"

    @pytest.mark.parametrize(
        "input,expected",
        [
            (DummyClass.meth, "test_marshalling:DummyClass.meth"),
            (DummyClass.classmeth, "test_marshalling:DummyClass.classmeth"),
            (
                DummyClass.InnerDummyClass.innerclassmeth,
                "test_marshalling:DummyClass.InnerDummyClass.innerclassmeth",
            ),
            (DummyClass.staticmeth, "test_marshalling:DummyClass.staticmeth"),
            (
                InheritedDummyClass.classmeth,
                "test_marshalling:InheritedDummyClass.classmeth",
            ),
            (timedelta, "datetime:timedelta"),
        ],
        ids=[
            "unbound method",
            "class method",
            "inner class method",
            "static method",
            "inherited class method",
            "timedelta",
        ],
    )
    def test_valid_refs(self, input, expected):
        assert callable_to_ref(input) == expected


class TestCallableFromRef:
    def test_valid_ref(self):
        from logging.handlers import RotatingFileHandler

        assert (
            callable_from_ref("logging.handlers:RotatingFileHandler")
            is RotatingFileHandler
        )

    def test_complex_path(self):
        pkg1 = ModuleType("pkg1")
        pkg1.pkg2 = "blah"
        pkg2 = ModuleType("pkg1.pkg2")
        pkg2.varname = lambda: None
        sys.modules["pkg1"] = pkg1
        sys.modules["pkg1.pkg2"] = pkg2
        assert callable_from_ref("pkg1.pkg2:varname") == pkg2.varname

    @pytest.mark.parametrize(
        "input,error",
        [(object(), TypeError), ("module", ValueError), ("module:blah", LookupError)],
        ids=["raw object", "module", "module attribute"],
    )
    def test_lookup_error(self, input, error):
        pytest.raises(error, callable_from_ref, input)
