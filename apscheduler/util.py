"""This module contains several handy functions primarily meant for internal use."""

from __future__ import division
from datetime import date, datetime, timedelta, tzinfo
from calendar import timegm
import re

from dateutil.tz import gettz, tzutc
import six

__all__ = ('asint', 'asbool', 'astimezone', 'convert_to_datetime', 'datetime_to_utc_timestamp',
           'utc_timestamp_to_datetime', 'timedelta_seconds', 'datetime_ceil', 'combine_opts', 'get_callable_name',
           'obj_to_ref', 'ref_to_obj', 'maybe_ref')


def asint(text):
    """
    Safely converts a string to an integer, returning None if the string is None.

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


def astimezone(obj):
    """
    Interprets an object as a timezone.

    :rtype: tzinfo
    """

    if isinstance(obj, six.string_types):
        return gettz(obj)
    if isinstance(obj, tzinfo):
        return obj
    if obj is not None:
        raise TypeError('Expected tzinfo, got %s instead' % obj.__class__.__name__)


_DATE_REGEX = re.compile(
    r'(?P<year>\d{4})-(?P<month>\d{1,2})-(?P<day>\d{1,2})'
    r'(?: (?P<hour>\d{1,2}):(?P<minute>\d{1,2}):(?P<second>\d{1,2})'
    r'(?:\.(?P<microsecond>\d{1,6}))?)?')


def convert_to_datetime(input, timezone, arg_name):
    """
    Converts the given object to a timezone aware datetime object.
    If a timezone aware datetime object is passed, it is returned unmodified.
    If a native datetime object is passed, it is given the specified timezone.
    If the input is a string, it is parsed as a datetime with the given timezone.

    Date strings are accepted in three different forms: date only (Y-m-d),
    date with time (Y-m-d H:M:S) or with date+time with microseconds
    (Y-m-d H:M:S.micro).

    :rtype: datetime
    """

    if isinstance(input, datetime):
        datetime_ = input
    elif isinstance(input, date):
        datetime_ = datetime.fromordinal(input.toordinal())
    elif isinstance(input, six.string_types):
        m = _DATE_REGEX.match(input)
        if not m:
            raise ValueError('Invalid date string')
        values = [(k, int(v or 0)) for k, v in m.groupdict().items()]
        values = dict(values)
        datetime_ = datetime(**values)
    else:
        raise TypeError('Unsupported input type: %s' % type(input))

    if datetime_.tzinfo is not None:
        return datetime_
    if timezone is None:
        raise ValueError('The "timezone" argument must be specified if %s has no timezone information' % arg_name)
    if isinstance(timezone, six.string_types):
        timezone = gettz(timezone)

    # When working with pytz timezone's you should use the localize
    # method when converting an offset-naive datetime. For all other
    # timezones (typically non-DST observing) it is safe to override
    # tzinfo with the specified timezone.
    #
    # For more information read the pytz manual: http://pytz.sourceforge.net/
    if hasattr(timezone, 'localize'):
        datetime_ = timezone.localize(datetime_)
    else:
        datetime_ = datetime_.replace(tzinfo=timezone)

    return datetime_


def datetime_to_utc_timestamp(timeval):
    if timeval is not None:
        return timegm(timeval.utctimetuple()) + timeval.microsecond / 1000000


def utc_timestamp_to_datetime(timestamp):
    if timestamp is not None:
        return datetime.fromtimestamp(timestamp, tzutc())


def timedelta_seconds(delta):
    """
    Converts the given timedelta to seconds.

    :type delta: timedelta
    :rtype: float
    """

    return delta.days * 24 * 60 * 60 + delta.seconds + \
        delta.microseconds / 1000000.0


def datetime_ceil(dateval):
    """
    Rounds the given datetime object upwards.

    :type dateval: datetime
    """

    if dateval.microsecond > 0:
        return dateval + timedelta(seconds=1,
                                   microseconds=-dateval.microsecond)
    return dateval


def datetime_repr(dateval):
    return dateval.strftime('%Y-%m-%d %H:%M:%S %Z') if dateval else 'None'


def combine_opts(global_config, prefix, local_config={}):
    """
    Returns a subdictionary from keys and values of  ``global_config`` where the key starts with the given prefix,
    combined with options from local_config. The keys in the subdictionary have the prefix removed.

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

    :rtype: str
    """

    f_self = getattr(func, '__self__', None) or getattr(func, 'im_self', None)

    if f_self and hasattr(func, '__name__'):
        if isinstance(f_self, type):
            # class method
            clsname = getattr(f_self, '__qualname__', None) or f_self.__name__
            return '%s.%s' % (clsname, func.__name__)
        # bound method
        return '%s.%s' % (f_self.__class__.__name__, func.__name__)

    if hasattr(func, '__call__'):
        if hasattr(func, '__name__'):
            # function, unbound method or a class with a __call__ method
            return func.__name__
        # instance of a class with a __call__ method
        return func.__class__.__name__

    raise TypeError('Unable to determine a name for %r -- maybe it is not a callable?' % func)


def obj_to_ref(obj):
    """
    Returns the path to the given object.

    :rtype: str
    """

    try:
        ref = '%s:%s' % (obj.__module__, get_callable_name(obj))
        obj2 = ref_to_obj(ref)
        if obj != obj2:
            raise ValueError
    except Exception:
        raise ValueError('Cannot determine the reference to %r' % obj)

    return ref


def ref_to_obj(ref):
    """
    Returns the object pointed to by ``ref``.

    :type ref: str
    """

    if not isinstance(ref, six.string_types):
        raise TypeError('References must be strings')
    if ':' not in ref:
        raise ValueError('Invalid reference')

    modulename, rest = ref.split(':', 1)
    try:
        obj = __import__(modulename)
    except ImportError:
        raise LookupError('Error resolving reference %s: could not import module' % ref)

    try:
        for name in modulename.split('.')[1:] + rest.split('.'):
            obj = getattr(obj, name)
        return obj
    except Exception:
        raise LookupError('Error resolving reference %s: error looking up object' % ref)


def maybe_ref(ref):
    """
    Returns the object that the given reference points to, if it is indeed a reference.
    If it is not a reference, the object is returned as-is.
    """

    if not isinstance(ref, str):
        return ref
    return ref_to_obj(ref)


if six.PY2:
    def repr_escape(string):
        if isinstance(string, six.text_type):
            return string.encode('ascii', 'backslashreplace')
        return string
else:
    repr_escape = lambda string: string
