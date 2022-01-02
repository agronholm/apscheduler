from __future__ import annotations

from datetime import datetime, timedelta
from enum import Enum
from uuid import UUID

from . import abc


def as_aware_datetime(value: datetime | str) -> datetime | None:
    """Convert the value from a string to a timezone aware datetime."""
    if isinstance(value, str):
        # fromisoformat() does not handle the "Z" suffix
        if value.upper().endswith('Z'):
            value = value[:-1] + '+00:00'

        value = datetime.fromisoformat(value)

    return value


def as_uuid(value: UUID | str) -> UUID:
    """Convert a string-formatted UUID to a UUID instance."""
    if isinstance(value, str):
        return UUID(value)

    return value


def as_timedelta(value: timedelta | float | None) -> timedelta:
    if isinstance(value, (float, int)):
        return timedelta(seconds=value)

    return value


def as_enum(enum_class: type[Enum]):
    def converter(value: enum_class | str):
        if isinstance(value, str):
            return enum_class.__members__[value]

        return value

    return converter


def as_async_datastore(value: abc.DataStore | abc.AsyncDataStore) -> abc.AsyncDataStore:
    if isinstance(value, abc.DataStore):
        from apscheduler.datastores.async_adapter import AsyncDataStoreAdapter
        return AsyncDataStoreAdapter(value)

    return value
