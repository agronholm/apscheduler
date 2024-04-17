"""This module contains several handy functions primarily meant for internal use."""

from __future__ import annotations

import sys
from datetime import datetime, tzinfo
from typing import Any, NoReturn, TypeVar

from ._exceptions import DeserializationError
from .abc import Trigger

if sys.version_info >= (3, 9):
    from zoneinfo import ZoneInfo
else:
    from backports.zoneinfo import ZoneInfo

T = TypeVar("T")


class UnsetValue:
    """The type of :data:`unset`."""

    __slots__ = ()

    def __new__(cls) -> UnsetValue:
        try:
            return unset
        except NameError:
            return super().__new__(cls)

    def __getstate__(self) -> NoReturn:
        raise RuntimeError("Internal error: attempted to serialize an unset value")

    def __repr__(self) -> str:
        return "<unset>"


unset = UnsetValue()


def timezone_repr(timezone: tzinfo) -> str:
    if isinstance(timezone, ZoneInfo):
        return timezone.key
    else:
        return repr(timezone)


def absolute_datetime_diff(dateval1: datetime, dateval2: datetime) -> float:
    return dateval1.timestamp() - dateval2.timestamp()


def qualified_name(cls: type) -> str:
    module = getattr(cls, "__module__", None)
    if module is None or module == "builtins":
        return cls.__qualname__
    else:
        return f"{module}.{cls.__qualname__}"


def require_state_version(
    trigger: Trigger, state: dict[str, Any], max_version: int
) -> None:
    try:
        if state["version"] > max_version:
            raise DeserializationError(
                f"{trigger.__class__.__name__} received a serialized state with "
                f'version {state["version"]}, but it only supports up to version '
                f"{max_version}. This can happen when an older version of APScheduler "
                f"is being used with a data store that was previously used with a "
                f"newer APScheduler version."
            )
    except KeyError as exc:
        raise DeserializationError(
            'Missing "version" key in the serialized state'
        ) from exc
