from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone, tzinfo
from typing import Any
from uuid import UUID
from zoneinfo import ZoneInfo

from tzlocal import get_localzone


def as_int(value: int | str) -> int:
    if isinstance(value, str):
        return int(value)

    return value


def as_aware_datetime(value: datetime | str) -> datetime:
    if isinstance(value, str):
        # Before Python 3.11, fromisoformat() could not handle the "Z" suffix
        if value.upper().endswith("Z"):
            value = value[:-1] + "+00:00"

        value = datetime.fromisoformat(value)

    if isinstance(value, datetime) and value.tzinfo is None:
        value = value.astimezone(get_localzone())

    return value


def as_date(value: date | str) -> date:
    if isinstance(value, str):
        return date.fromisoformat(value)

    return value


def as_timezone(value: tzinfo | str) -> tzinfo:
    if isinstance(value, str):
        return get_localzone() if value == "local" else ZoneInfo(value)
    elif value is timezone.utc:
        return ZoneInfo("UTC")

    return value


def as_uuid(value: UUID | str) -> UUID:
    if isinstance(value, str):
        return UUID(value)

    return value


def as_timedelta(value: timedelta | int) -> timedelta:
    if isinstance(value, (float, int)):
        return timedelta(seconds=value)

    return value


def as_enum(enum_class: Any) -> Callable[[Any], Any]:
    def converter(value: Any) -> Any:
        if isinstance(value, str):
            return enum_class[value]

        return value

    return converter


def list_converter(converter: Callable[[Any], Any]) -> Callable[[Any], Any]:
    def convert(value: Any) -> Any:
        if isinstance(value, list):
            return [converter(item) for item in value]

        return value

    return convert
