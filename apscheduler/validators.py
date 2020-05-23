from datetime import datetime, timedelta, tzinfo, date, timezone
from typing import Dict, Any, Optional, Union

import pytz
from dateutil.parser import parse
from tzlocal import get_localzone

from apscheduler.abc import Trigger
from apscheduler.exceptions import DeserializationError


def as_int(value) -> Optional[int]:
    """Convert the value into an integer."""
    if value is None:
        return None

    return int(value)


def as_timezone(value: Union[str, tzinfo, None]) -> tzinfo:
    """
    Convert the value into a pytz timezone.

    :param value: the value to be converted
    :return: a timezone object, or if ``None`` was given, the local timezone

    """
    if value is None or value == 'local':
        return get_localzone()
    elif isinstance(value, str):
        return pytz.timezone(value)
    elif isinstance(value, tzinfo):
        if value is timezone.utc:
            return pytz.utc
        elif not getattr(value, 'zone', None):
            raise TypeError('Only named pytz timezones are supported')
        else:
            return value

    raise TypeError(f'Expected pytz timezone or timezone.utc, got {value.__class__.__qualname__}'
                    f'instead')


def as_date(value: Union[date, str, None]) -> Optional[date]:
    """
    Convert the value to a date.

    :param value: the value to convert to a date
    :return: a date object, or ``None`` if ``None`` was given

    """
    if value is None:
        return None
    elif isinstance(value, int):
        return date.fromordinal(value)
    elif isinstance(value, str):
        return parse(value).date()
    elif isinstance(value, datetime):
        return value.date()
    elif isinstance(value, date):
        return value

    raise TypeError(f'Expected string or date, got {value.__class__.__qualname__} instead')


def as_timestamp(value: Optional[datetime]) -> Optional[float]:
    if value is None:
        return None

    return value.timestamp()


def as_ordinal_date(value: Optional[date]) -> Optional[int]:
    if value is None:
        return None

    return value.toordinal()


def as_aware_datetime(value: Union[datetime, str, float, None], tz: tzinfo) -> Optional[datetime]:
    """
    Convert the value to a timezone aware datetime.

    :param value: a datetime, an ISO 8601 representation of a datetime, or ``None``
    :param tz: timezone to use for making the datetime timezone aware
    :return: a timezone aware datetime, or ``None`` if ``None`` was given

    """
    if value is None:
        return None

    if isinstance(value, float):
        return datetime.fromtimestamp(value, tz)

    if isinstance(value, str):
        value = parse(value)

    if isinstance(value, datetime):
        if value.tzinfo:
            return value.astimezone(tz)

        try:
            # Works with pytz timezones
            return tz.localize(value)
        except AttributeError:
            # Not a pytz timezone
            return value.astimezone(tz)

    raise TypeError(f'Expected string or datetime, got {value.__class__.__qualname__} instead')


def positive_number(instance, attribute, value) -> None:
    if value <= 0:
        raise ValueError(f'Expected positive number, got {value} instead')


def non_negative_number(instance, attribute, value) -> None:
    if value < 0:
        raise ValueError(f'Expected non-negative number, got {value} instead')


def as_positive_integer(value, name: str) -> int:
    if isinstance(value, int):
        if value > 0:
            return value
        else:
            raise ValueError(f'{name} must be positive')

    raise TypeError(f'{name} must be an integer, got {value.__class__.__name__} instead')


def as_timedelta(value, name: str) -> timedelta:
    if isinstance(value, (int, float)):
        value = timedelta(seconds=value)

    if isinstance(value, timedelta):
        if value.total_seconds() < 0:
            raise ValueError(f'{name} cannot be negative')
        else:
            return value

    raise TypeError(f'{name} must be a timedelta or number of seconds, got '
                    f'{value.__class__.__name__} instead')


def as_list(value, element_type: type, name: str) -> list:
    value = list(value)
    for i, element in enumerate(value):
        if not isinstance(element, element_type):
            raise TypeError(f'Element at index {i} of {name} is not of the expected type '
                            f'({element_type.__name__}')

    return value


def require_state_version(trigger: Trigger, state: Dict[str, Any], max_version: int) -> None:
    try:
        if state['version'] > max_version:
            raise DeserializationError(
                f'{trigger.__class__.__name__} received a serialized state with version '
                f'{state["version"]}, but it only supports up to version {max_version}. '
                f'This can happen when an older version of APScheduler is being used with a data '
                f'store that was previously used with a newer APScheduler version.'
            )
    except KeyError as exc:
        raise DeserializationError('Missing "version" key in the serialized state') from exc
