"""This module contains several handy functions primarily meant for internal use."""
import sys
from datetime import datetime, tzinfo

if sys.version_info >= (3, 9):
    from zoneinfo import ZoneInfo
else:
    from backports.zoneinfo import ZoneInfo


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
