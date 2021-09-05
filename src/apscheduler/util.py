"""This module contains several handy functions primarily meant for internal use."""
from __future__ import annotations

import sys
from collections import defaultdict
from datetime import datetime, tzinfo
from typing import Callable, TypeVar

if sys.version_info >= (3, 9):
    from zoneinfo import ZoneInfo
else:
    from backports.zoneinfo import ZoneInfo

T_Type = TypeVar('T_Type', bound=type)


class _Undefined:
    def __bool__(self):
        return False

    def __repr__(self):
        return '<undefined>'


undefined = _Undefined()  #: a unique object that only signifies that no value is defined


def timezone_repr(timezone: tzinfo) -> str:
    if isinstance(timezone, ZoneInfo):
        return timezone.key
    else:
        return repr(timezone)


def absolute_datetime_diff(dateval1: datetime, dateval2: datetime) -> float:
    return dateval1.timestamp() - dateval2.timestamp()


def reentrant(cls: T_Type) -> T_Type:
    """
    Modifies a class so that its ``__enter__`` / ``__exit__`` (or ``__aenter__`` / ``__aexit__``)
    methods track the number of times it has been entered and exited and only actually invoke
    the ``__enter__()`` method on the first entry and ``__exit__()`` on the last exit.

    """

    def __enter__(self):
        loans[self] += 1
        if loans[self] == 1:
            previous_enter(self)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        assert loans[self]
        loans[self] -= 1
        if loans[self] == 0:
            return previous_exit(self, exc_type, exc_val, exc_tb)

    async def __aenter__(self):
        loans[self] += 1
        if loans[self] == 1:
            await previous_aenter(self)

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        assert loans[self]
        loans[self] -= 1
        if loans[self] == 0:
            del loans[self]
            return await previous_aexit(self, exc_type, exc_val, exc_tb)

    loans: dict[T_Type, int] = defaultdict(lambda: 0)
    previous_enter: Callable = getattr(cls, '__enter__', None)
    previous_exit: Callable = getattr(cls, '__exit__', None)
    previous_aenter: Callable = getattr(cls, '__aenter__', None)
    previous_aexit: Callable = getattr(cls, '__aexit__', None)
    if previous_enter and previous_exit:
        cls.__enter__ = __enter__
        cls.__exit__ = __exit__
    elif previous_aenter and previous_aexit:
        cls.__aenter__ = __aenter__
        cls.__aexit__ = __aexit__

    return cls
