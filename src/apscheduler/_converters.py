from __future__ import annotations

import sys
from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from tzlocal import get_localzone

if sys.version_info >= (3, 9):
    from zoneinfo import ZoneInfo
else:
    from backports.zoneinfo import ZoneInfo


def as_int(value: Any) -> Any:
    if isinstance(value, str):
        return int(value)

    return value


def as_aware_datetime(value: Any) -> Any:
    if isinstance(value, str):
        # Before Python 3.11, fromisoformat() could not handle the "Z" suffix
        if value.upper().endswith("Z"):
            value = value[:-1] + "+00:00"

        value = datetime.fromisoformat(value)

    if isinstance(value, datetime) and value.tzinfo is None:
        value = value.astimezone(get_localzone())

    return value


def as_date(value: Any) -> Any:
    if isinstance(value, str):
        return date.fromisoformat(value)

    return value


def as_timezone(value: Any) -> Any:
    if isinstance(value, str):
        if value is None or value == "local":
            return get_localzone()

        return ZoneInfo(value)
    elif value is timezone.utc:
        return ZoneInfo("UTC")

    return value


def as_uuid(value: Any) -> Any:
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
