from time import mktime
from calendar import monthrange, weekday

__all__ = ('min_values', 'max_values', 'asint', 'get_actual_maximum',
           'get_date_field', 'timedelta_seconds', 'time_difference')


min_values = {'year': 1970, 'month': 1, 'day': 1, 'day_of_week': 0,
              'hour': 0, 'minute': 0, 'second': 0}
max_values = {'year': 2 ** 63, 'month': 12, 'day:': 31, 'day_of_week': 6,
              'hour': 23, 'minute': 59, 'second': 59}


def asint(text):
    """
    Safely converts a string to an integer, returning None if the string
    is None.
    @type text: str
    @rtype: int
    """
    if text is not None:
        return int(text)


def get_actual_maximum(date, fieldname):
    """
    Retrieves the maximum applicable value for the given datetime field.
    @type date: datetime
    @type fieldname: str
    @rtype: int
    """
    if fieldname == 'day':
        return monthrange(date.year, date.month)[1]
    return max_values[fieldname]


def get_date_field(date, fieldname):
    """
    Extracts the value of the specified field from a datetime object.
    @type date: datetime
    @type fieldname: str
    @rtype: int
    """

    if fieldname == 'day_of_week':
        return weekday(date.year, date.month, date.day)
    return getattr(date, fieldname)


def timedelta_seconds(delta):
    """
    Converts the given timedelta to seconds.
    @type delta: timedelta
    """
    return delta.days * 24 * 60 * 60 + delta.seconds + \
        delta.microseconds / 100000.0


def time_difference(date1, date2):
    """
    Returns the time difference in seconds between the given two datetime objects.
    The difference is calculated as: date1 - date2.
    @param date1: the later datetime
    @type date1: datetime
    @param date2: the earlier datetime
    @type date2: datetime
    """
    
    return mktime(date1.timetuple()) - mktime(date2.timetuple())
