from dateutil.tz import tzlocal

from apscheduler.triggers.base import BaseTrigger
from apscheduler.util import convert_to_datetime, datetime_repr, astimezone


class DateTrigger(BaseTrigger):
    def __init__(self, run_date, timezone=None):
        """
        Triggers once on the given datetime.

        :param run_date: the date/time to run the job at
        :param timezone: time zone for ``run_date``
        :type timezone: str or an instance of a :cls:`~datetime.tzinfo` subclass
        """

        self.timezone = astimezone(timezone) or tzlocal()
        self.run_date = convert_to_datetime(run_date, self.timezone, 'run_date')

    def get_next_fire_time(self, start_date):
        if self.run_date >= start_date:
            # Make sure that the returned date is in the trigger
            # timezone. Also, has the additional benefit of normalizing
            # the returned datetime.
            return self.run_date.astimezone(self.timezone)

    def __str__(self):
        return 'date[%s]' % datetime_repr(self.run_date)

    def __repr__(self):
        return "<%s (run_date='%s')>" % (self.__class__.__name__, datetime_repr(self.run_date))
