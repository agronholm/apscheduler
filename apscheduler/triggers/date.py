from apscheduler.triggers.base import BaseTrigger
from apscheduler.util import convert_to_datetime, datetime_repr, astimezone


class DateTrigger(BaseTrigger):
    def __init__(self, timezone, run_date):
        """
        Triggers once on the given datetime.

        :param timezone: time zone for ``run_date``
        :param run_date: the date/time to run the job at
        :type timezone: str or an instance of a :cls:`~datetime.tzinfo` subclass
        """

        timezone = astimezone(timezone)
        self.run_date = convert_to_datetime(run_date, timezone, 'run_date')

    def get_next_fire_time(self, start_date):
        if self.run_date >= start_date:
            return self.run_date

    def __str__(self):
        return 'date[%s]' % datetime_repr(self.run_date)

    def __repr__(self):
        return "<%s (run_date='%s')>" % (self.__class__.__name__, datetime_repr(self.run_date))
