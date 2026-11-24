from __future__ import annotations

import re
from collections.abc import Callable
from datetime import UTC, timedelta, timezone, tzinfo
from functools import partial
from inspect import isclass, ismethod, ismethoddescriptor
from typing import Any
from zoneinfo import ZoneInfo

from ._exceptions import DeserializationError, SerializationError

_FIXED_OFFSET_PATTERN = re.compile(
    r"UTC(?P<sign>[+-])(?P<hours>\d{2}):(?P<minutes>\d{2})"
)


def marshal_object(obj) -> tuple[str, Any]:
    return (
        f"{obj.__class__.__module__}:{obj.__class__.__qualname__}",
        obj.__getstate__(),
    )


def unmarshal_object(ref: str, state: Any) -> Any:
    cls = callable_from_ref(ref)
    if not isinstance(cls, type):
        raise TypeError(f"{ref} is not a class")

    instance = cls.__new__(cls)
    instance.__setstate__(state)
    return instance


def _marshal_fixed_offset(value: timezone) -> str:
    """Represent a :class:`datetime.timezone` by its UTC offset."""
    offset = value.utcoffset(None)
    if offset == timedelta(0):
        return "UTC"

    total_minutes = offset // timedelta(minutes=1)
    if offset != total_minutes * timedelta(minutes=1):
        raise SerializationError(
            f"Cannot serialize time zone {value!r}: UTC offset must be a whole number "
            f"of minutes"
        )

    sign = "+" if total_minutes > 0 else "-"
    hours, minutes = divmod(abs(total_minutes), 60)
    return f"UTC{sign}{hours:02d}:{minutes:02d}"


def marshal_timezone(value: tzinfo) -> str:
    if isinstance(value, ZoneInfo):
        return value.key
    if hasattr(value, "zone"):  # pytz timezones
        return value.zone
    if isinstance(value, timezone):
        return _marshal_fixed_offset(value)

    raise SerializationError(
        f"Unserializable time zone: {value!r}\n"
        f"Only time zones from the zoneinfo or pytz modules, and fixed-offset "
        f"datetime.timezone instances, can be serialized."
    )


def unmarshal_timezone(value: str) -> tzinfo:
    if value == "UTC":
        return UTC

    if match := _FIXED_OFFSET_PATTERN.fullmatch(value):
        offset = timedelta(hours=int(match["hours"]), minutes=int(match["minutes"]))
        return timezone(-offset if match["sign"] == "-" else offset)

    return ZoneInfo(value)


def callable_to_ref(func: Callable) -> str:
    """
    Return a reference to the given callable.

    :raises SerializationError: if the given object is not callable, is a partial(),
        bound method, lambda or local function or does not have the ``__module__`` and
        ``__qualname__`` attributes

    """
    if isinstance(func, partial):
        raise SerializationError("Cannot create a reference to a partial()")

    if ismethod(func):
        if not isclass(func.__self__):
            raise SerializationError("Cannot create a reference to an instance method")

        return f"{func.__module__}:{func.__self__.__qualname__}.{func.__name__}"

    if ismethoddescriptor(func):
        return f"{func.__objclass__.__module__}:{func.__qualname__}"

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
    except ImportError as exc:
        raise LookupError(
            f"Error resolving reference {ref!r}: could not import module"
        ) from exc

    try:
        for name in rest.split("."):
            obj = getattr(obj, name)
    except Exception as exc:
        raise DeserializationError(
            f"Error resolving reference {ref!r}: error looking up object"
        ) from exc

    if not callable(obj):
        raise DeserializationError(
            f"{ref!r} points to an object of type "
            f"{obj.__class__.__qualname__} which is not callable"
        )

    return obj
