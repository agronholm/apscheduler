from __future__ import annotations

import sys
from datetime import timedelta
from functools import partial
from inspect import ismethod
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
        "binding", ["missing_module", "missing_class", "wrong_class", "wrong_method"]
    )
    def test_class_method_reference_must_resolve_to_original(
        self, monkeypatch: pytest.MonkeyPatch, binding: str
    ) -> None:
        module = ModuleType("generated_jobs")

        def run(cls):
            pass

        generated = type(
            "GeneratedWorker",
            (),
            {"run": classmethod(run), "__module__": module.__name__},
        )
        target = generated.run
        if binding != "missing_module":
            monkeypatch.setitem(sys.modules, module.__name__, module)
        if binding == "wrong_class":
            # Even the same function bound to a different class is not equivalent.
            module.GeneratedWorker = type("OtherWorker", (), {"run": classmethod(run)})
        elif binding == "wrong_method":
            module.GeneratedWorker = generated
            generated.run = DummyClass.classmeth

        with pytest.raises(SerializationError, match="class method"):
            callable_to_ref(target)

    @pytest.mark.parametrize("method_name", ["run", "run_async"])
    def test_cross_module_inherited_class_method(
        self, cross_module_job_classes: tuple[type, type], method_name: str
    ) -> None:
        for job_class in cross_module_job_classes:
            target = getattr(job_class, method_name)
            ref = callable_to_ref(target)
            assert ref == f"{job_class.__module__}:Worker.{method_name}"
            restored = callable_from_ref(ref)
            assert ismethod(restored)
            assert restored.__self__ is job_class
            assert restored.__func__ is target.__func__

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

    def test_local_class_method_error(self):
        class Local:
            @classmethod
            def run(cls):
                pass

        with pytest.raises(SerializationError, match="nested function"):
            callable_to_ref(Local.run)

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
