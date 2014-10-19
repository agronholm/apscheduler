from datetime import timedelta, datetime
from math import ceil

from tzlocal import get_localzone

from apscheduler.triggers.base import BaseTrigger
from apscheduler.util import convert_to_datetime, timedelta_seconds, datetime_repr, astimezone


class IntervalTrigger(BaseTrigger):
    """
    Triggers on specified intervals, starting on ``start_date`` if specified, ``datetime.now()`` + interval
    otherwise.

    :param int weeks: number of weeks to wait
    :param int days: number of days to wait
    :param int hours: number of hours to wait
    :param int minutes: number of minutes to wait
    :param int seconds: number of seconds to wait
    :param datetime|str start_date: starting point for the interval calculation
    :param datetime|str end_date: latest possible date/time to trigger on
    :param datetime.tzinfo|str timezone: time zone to use for the date/time calculations
    """

    __slots__ = 'timezone', 'start_date', 'end_date', 'interval'

    def __init__(self, weeks=0, days=0, hours=0, minutes=0, seconds=0, start_date=None, end_date=None, timezone=None):
        self.interval = timedelta(weeks=weeks, days=days, hours=hours, minutes=minutes, seconds=seconds)
        self.interval_length = timedelta_seconds(self.interval)
        if self.interval_length == 0:
            self.interval = timedelta(seconds=1)
            self.interval_length = 1

        if timezone:
            self.timezone = astimezone(timezone)
        elif start_date and start_date.tzinfo:
            self.timezone = start_date.tzinfo
        elif end_date and end_date.tzinfo:
            self.timezone = end_date.tzinfo
        else:
            self.timezone = get_localzone()

        start_date = start_date or (datetime.now(self.timezone) + self.interval)
        self.start_date = convert_to_datetime(start_date, self.timezone, 'start_date')
        self.end_date = convert_to_datetime(end_date, self.timezone, 'end_date')

    def get_next_fire_time(self, previous_fire_time, now):
        if previous_fire_time:
            next_fire_time = previous_fire_time + self.interval
        elif self.start_date > now:
            next_fire_time = self.start_date
        else:
            timediff_seconds = timedelta_seconds(now - self.start_date)
            next_interval_num = int(ceil(timediff_seconds / self.interval_length))
            next_fire_time = self.start_date + self.interval * next_interval_num

        if not self.end_date or next_fire_time <= self.end_date:
            return self.timezone.normalize(next_fire_time)

    def __str__(self):
        return 'interval[%s]' % str(self.interval)

    def __repr__(self):
        return "<%s (interval=%r, start_date='%s')>" % (self.__class__.__name__, self.interval,
                                                        datetime_repr(self.start_date))
