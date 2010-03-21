"""
This module contains several handy functions primarily meant for internal use.
"""

from datetime import date, datetime, timedelta
from calendar import monthrange, weekday
from time import strftime

__all__ = ('MIN_VALUES', 'MAX_VALUES', 'asint', 'asbool', 'get_actual_maximum',
           'get_date_field', 'convert_to_datetime', 'timedelta_seconds',
           'time_difference', 'datetime_ceil')


MIN_VALUES = {'year': 1970, 'month': 1, 'day': 1, 'week': 1,
              'day_of_week': 0, 'hour': 0, 'minute': 0, 'second': 0}
MAX_VALUES = {'year': 2 ** 63, 'month': 12, 'day:': 31, 'week': 53,
              'day_of_week': 6, 'hour': 23, 'minute': 59, 'second': 59}


def asint(text):
    """
    Safely converts a string to an integer, returning None if the string
    is None.

    :type text: str
    :rtype: int
    """
    if text is not None:
        return int(text)


def asbool(obj):
    """
    Interprets an object as a boolean value.

    :rtype: bool
    """
    if isinstance(obj, str):
        obj = obj.strip().lower()
        if obj in ('true', 'yes', 'on', 'y', 't', '1'):
            return True
        if obj in ('false', 'no', 'off', 'n', 'f', '0'):
            return False
        raise ValueError('Unable to interpret value "%s" as boolean' % obj)
    return bool(obj)


def get_actual_maximum(dateval, fieldname):
    """
    Retrieves the maximum applicable value for the given datetime field.

    :type dateval: datetime
    :type fieldname: str
    :rtype: int
    """
    if fieldname == 'day':
        return monthrange(dateval.year, dateval.month)[1]
    return MAX_VALUES[fieldname]


def get_date_field(dateval, fieldname):
    """
    Extracts the value of the specified field from a datetime object.

    :type dateval: datetime
    :type fieldname: str
    :rtype: int
    """
    if fieldname == 'week':
        week_str = strftime('%W', dateval.timetuple())
        return int(week_str) + 1
    if fieldname == 'day_of_week':
        return weekday(dateval.year, dateval.month, dateval.day)
    return getattr(dateval, fieldname)


def convert_to_datetime(dateval):
    """
    Converts a date object to a datetime object.
    If an actual datetime object is passed, it is returned unmodified.

    :type dateval: date
    :rtype: datetime
    """
    if isinstance(dateval, datetime):
        return dateval
    elif isinstance(dateval, date):
        return datetime.fromordinal(dateval.toordinal())
    raise TypeError('Expected date, got %s instead' % type(dateval))


def timedelta_seconds(delta):
    """
    Converts the given timedelta to seconds.

    :type delta: timedelta
    :rtype: float
    """
    return delta.days * 24 * 60 * 60 + delta.seconds + \
        delta.microseconds / 1000000.0


def time_difference(date1, date2):
    """
    Returns the time difference in seconds between the given two
    datetime objects. The difference is calculated as: date1 - date2.

    :param date1: the later datetime
    :type date1: datetime
    :param date2: the earlier datetime
    :type date2: datetime
    :rtype: float
    """
    if date1 >= date2:
        return timedelta_seconds(date1 - date2)
    return -timedelta_seconds(date2 - date1)


def datetime_ceil(dateval):
    """
    Rounds the given datetime object upwards.

    :type dateval: datetime
    """
    if dateval.microsecond > 0:
        return dateval + timedelta(seconds=1,
                                   microseconds=-dateval.microsecond)
    return dateval
