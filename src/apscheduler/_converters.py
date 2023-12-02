from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, TypeVar
from uuid import UUID

TEnum = TypeVar("TEnum", bound=Enum)
T = TypeVar("T")


def as_aware_datetime(value: Any) -> Any:
    """Convert the value from a string to a timezone aware datetime."""
    if isinstance(value, str):
        # Before Python 3.11, fromisoformat() could not handle the "Z" suffix
        if value.upper().endswith("Z"):
            value = value[:-1] + "+00:00"

        value = datetime.fromisoformat(value)

    return value


def as_uuid(value: Any) -> Any:
    """Convert a string-formatted UUID to a UUID instance."""
    if isinstance(value, str):
        return UUID(value)

    return value


def as_timedelta(value: Any) -> Any:
    if isinstance(value, (float, int)):
        return timedelta(seconds=value)

    return value


def as_enum(enum_class: Any) -> Callable[[Any], Any]:
    def converter(value: Any) -> Any:
        if isinstance(value, str):
            return enum_class.__members__[value]

        return value

    return converter


def list_converter(converter: Callable[[Any], Any]) -> Callable[[Any], Any]:
    def convert(value: Any) -> Any:
        if isinstance(value, list):
            return [converter(item) for item in value]

        return value

    return convert
