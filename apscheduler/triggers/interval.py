from datetime import datetime, timedelta
from math import ceil

from apscheduler.util import convert_to_datetime, timedelta_seconds


class IntervalTrigger(object):
    def __init__(self, interval, start_date=None):
        if not isinstance(interval, timedelta):
            raise TypeError('interval must be a timedelta')

        self.interval = interval
        self.interval_length = timedelta_seconds(self.interval)
        if self.interval_length == 0:
            self.interval = timedelta(seconds=1)
            self.interval_length = 1

        if start_date is None:
            self.first_fire_date = datetime.now() + self.interval
        else:
            self.first_fire_date = convert_to_datetime(start_date)

    def get_next_fire_time(self, start_date):
        if start_date < self.first_fire_date:
            return self.first_fire_date

        timediff_seconds = timedelta_seconds(start_date - self.first_fire_date)
        next_interval_num = int(ceil(timediff_seconds / self.interval_length))
        return self.first_fire_date + self.interval * next_interval_num

    def __repr__(self):
        return '%s(interval=%s, start_date=%s)' % (self.__class__.__name__,
                                                   repr(self.interval),
                                                   repr(self.first_fire_date))
