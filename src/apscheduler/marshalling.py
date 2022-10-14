from __future__ import annotations

import sys
from datetime import date, datetime, tzinfo
from functools import partial
from typing import Any, Callable, overload

from ._exceptions import DeserializationError, SerializationError

if sys.version_info >= (3, 9):
    from zoneinfo import ZoneInfo
else:
    from backports.zoneinfo import ZoneInfo


def marshal_object(obj) -> tuple[str, Any]:
    return (
        f"{obj.__class__.__module__}:{obj.__class__.__qualname__}",
        obj.__getstate__(),
    )


def unmarshal_object(ref: str, state):
    cls = callable_from_ref(ref)
    instance = cls.__new__(cls)
    instance.__setstate__(state)
    return instance


@overload
def marshal_date(value: None) -> None:
    ...


@overload
def marshal_date(value: date) -> str:
    ...


def marshal_date(value):
    return value.isoformat() if value is not None else None


@overload
def unmarshal_date(value: None) -> None:
    ...


@overload
def unmarshal_date(value: str) -> date:
    ...


def unmarshal_date(value):
    if value is None:
        return None
    elif len(value) == 10:
        return date.fromisoformat(value)
    else:
        return datetime.fromisoformat(value)


def marshal_timezone(value: tzinfo) -> str:
    if isinstance(value, ZoneInfo):
        return value.key
    elif hasattr(value, "zone"):  # pytz timezones
        return value.zone

    raise SerializationError(
        f"Unserializable time zone: {value!r}\n"
        f"Only time zones from the zoneinfo or pytz modules can be serialized."
    )


def unmarshal_timezone(value: str) -> ZoneInfo:
    return ZoneInfo(value)


def callable_to_ref(func: Callable) -> str:
    """
    Return a reference to the given callable.

    :raises SerializationError: if the given object is not callable, is a partial(),
        lambda or local function or does not have the ``__module__`` and
        ``__qualname__`` attributes

    """
    if isinstance(func, partial):
        raise SerializationError("Cannot create a reference to a partial()")

    if not hasattr(func, "__module__"):
        raise SerializationError("Callable has no __module__ attribute")
    if not hasattr(func, "__qualname__"):
        raise SerializationError("Callable has no __qualname__ attribute")
    if "<lambda>" in func.__qualname__:
        raise SerializationError("Cannot create a reference to a lambda")
    if "<locals>" in func.__qualname__:
        raise SerializationError("Cannot create a reference to a nested function")

    return f"{func.__module__}:{func.__qualname__}"


def callable_from_ref(ref: str) -> Callable:
    """
    Return the callable pointed to by ``ref``.

    :raises DeserializationError: if the reference could not be resolved or the looked
        up object is not callable

    """
    if ":" not in ref:
        raise ValueError(f"Invalid reference: {ref}")

    modulename, rest = ref.split(":", 1)
    try:
        obj = __import__(modulename, fromlist=[rest])
    except ImportError:
        raise LookupError(f"Error resolving reference {ref!r}: could not import module")

    try:
        for name in rest.split("."):
            obj = getattr(obj, name)
    except Exception:
        raise DeserializationError(
            f"Error resolving reference {ref!r}: error looking up object"
        )

    if not callable(obj):
        raise DeserializationError(
            f"{ref!r} points to an object of type "
            f"{obj.__class__.__qualname__} which is not callable"
        )

    return obj
