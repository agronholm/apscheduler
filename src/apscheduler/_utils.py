"""This module contains several handy functions primarily meant for internal use."""
from __future__ import annotations

import sys
from datetime import datetime, tzinfo
from typing import TypeVar

if sys.version_info >= (3, 9):
    from zoneinfo import ZoneInfo
else:
    from backports.zoneinfo import ZoneInfo

T = TypeVar("T")


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
