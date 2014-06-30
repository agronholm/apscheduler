from datetime import datetime

from tzlocal import get_localzone

from apscheduler.triggers.base import BaseTrigger
from apscheduler.util import convert_to_datetime, datetime_repr, astimezone


class DateTrigger(BaseTrigger):
    """
    Triggers once on the given datetime. If ``run_date`` is left empty, current time is used.

    :param datetime|str run_date: the date/time to run the job at
    :param datetime.tzinfo|str timezone: time zone for ``run_date`` if it doesn't have one already
    """

    __slots__ = 'timezone', 'run_date'

    def __init__(self, run_date=None, timezone=None):
        timezone = astimezone(timezone) or get_localzone()
        self.run_date = convert_to_datetime(run_date or datetime.now(), timezone, 'run_date')

    def get_next_fire_time(self, previous_fire_time, now):
        return self.run_date if previous_fire_time is None else None

    def __str__(self):
        return 'date[%s]' % datetime_repr(self.run_date)

    def __repr__(self):
        return "<%s (run_date='%s')>" % (self.__class__.__name__, datetime_repr(self.run_date))
