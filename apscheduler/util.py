"""
This module contains several handy functions primarily meant for internal use.
"""

from datetime import date, datetime, timedelta
from time import mktime
import re
import sys

__all__ = ('asint', 'asbool', 'convert_to_datetime', 'timedelta_seconds',
           'time_difference', 'datetime_ceil', 'combine_opts',
           'get_callable_name', 'obj_to_ref', 'ref_to_obj', 'maybe_ref',
           'to_unicode', 'iteritems', 'itervalues', 'xrange')


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


_DATE_REGEX = re.compile(
    r'(?P<year>\d{4})-(?P<month>\d{1,2})-(?P<day>\d{1,2})'
    r'(?: (?P<hour>\d{1,2}):(?P<minute>\d{1,2}):(?P<second>\d{1,2})'
    r'(?:\.(?P<microsecond>\d{1,6}))?)?')


def convert_to_datetime(input):
    """
    Converts the given object to a datetime object, if possible.
    If an actual datetime object is passed, it is returned unmodified.
    If the input is a string, it is parsed as a datetime.

    Date strings are accepted in three different forms: date only (Y-m-d),
    date with time (Y-m-d H:M:S) or with date+time with microseconds
    (Y-m-d H:M:S.micro).

    :rtype: datetime
    """
    if isinstance(input, datetime):
        return input
    elif isinstance(input, date):
        return datetime.fromordinal(input.toordinal())
    elif isinstance(input, str):
        m = _DATE_REGEX.match(input)
        if not m:
            raise ValueError('Invalid date string')
        values = [(k, int(v or 0)) for k, v in m.groupdict().items()]
        values = dict(values)
        return datetime(**values)
    raise TypeError('Unsupported input type: %s' % type(input))


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
    later = mktime(date1.timetuple()) + date1.microsecond / 1000000.0
    earlier = mktime(date2.timetuple()) + date2.microsecond / 1000000.0
    return later - earlier


def datetime_ceil(dateval):
    """
    Rounds the given datetime object upwards.

    :type dateval: datetime
    """
    if dateval.microsecond > 0:
        return dateval + timedelta(seconds=1,
                                   microseconds=-dateval.microsecond)
    return dateval


def combine_opts(global_config, prefix, local_config={}):
    """
    Returns a subdictionary from keys and values of  ``global_config`` where
    the key starts with the given prefix, combined with options from
    local_config. The keys in the subdictionary have the prefix removed.

    :type global_config: dict
    :type prefix: str
    :type local_config: dict
    :rtype: dict
    """
    prefixlen = len(prefix)
    subconf = {}
    for key, value in global_config.items():
        if key.startswith(prefix):
            key = key[prefixlen:]
            subconf[key] = value
    subconf.update(local_config)
    return subconf


def get_callable_name(func):
    """
    Returns the best available display name for the given function/callable.
    """
    name = func.__module__
    if hasattr(func, '__self__') and func.__self__:
        name += '.' + func.__self__.__name__
    elif hasattr(func, 'im_self') and func.im_self:     # py2.4, 2.5
        name += '.' + func.im_self.__name__
    if hasattr(func, '__name__'):
        name += '.' + func.__name__
    return name


def obj_to_ref(obj):
    """
    Returns the path to the given object.
    """
    ref = '%s:%s' % (obj.__module__, obj.__name__)
    try:
        obj2 = ref_to_obj(ref)
    except AttributeError:
        pass
    else:
        if obj2 == obj:
            return ref

    raise ValueError('Only module level objects are supported')


def ref_to_obj(ref):
    """
    Returns the object pointed to by ``ref``.
    """
    modulename, rest = ref.split(':', 1)
    obj = __import__(modulename)
    for name in modulename.split('.')[1:] + rest.split('.'):
        obj = getattr(obj, name)
    return obj


def maybe_ref(ref):
    """
    Returns the object that the given reference points to, if it is indeed
    a reference. If it is not a reference, the object is returned as-is.
    """
    if not isinstance(ref, str):
        return ref
    return ref_to_obj(ref)


def to_unicode(string, encoding='ascii'):
    """
    Safely converts a string to a unicode representation on any
    Python version.
    """
    if hasattr(string, 'decode'):
        return string.decode(encoding, 'ignore')
    return string


if sys.version_info < (3, 0):  # pragma: nocover
    iteritems = lambda d: d.iteritems()
    itervalues = lambda d: d.itervalues()
    xrange = xrange
else:  # pragma: nocover
    iteritems = lambda d: d.items()
    itervalues = lambda d: d.values()
    xrange = range
