"""
Triggers determine the times when a job should be executed.
"""
from datetime import datetime, timedelta
from math import ceil

from apscheduler.fields import *
from apscheduler.util import *

__all__ = ('CronTrigger', 'DateTrigger', 'IntervalTrigger')


class CronTrigger(object):
    FIELD_NAMES = ('year', 'month', 'day', 'week', 'day_of_week', 'hour',
                   'minute', 'second')
    FIELDS_MAP = {'year': BaseField,
                  'month': BaseField,
                  'week': WeekField,
                  'day': DayOfMonthField,
                  'day_of_week': DayOfWeekField,
                  'hour': BaseField,
                  'minute': BaseField,
                  'second': BaseField}

    def __init__(self, **values):
        self.fields = []
        for field_name in self.FIELD_NAMES:
            exprs = values.get(field_name) or '*'
            field_class = self.FIELDS_MAP[field_name]
            field = field_class(field_name, exprs)
            self.fields.append(field)

    def _increment_field_value(self, dateval, fieldnum):
        """
        Increments the designated field and resets all less significant fields
        to their minimum values.

        :type dateval: datetime
        :type fieldnum: int
        :type amount: int
        :rtype: tuple
        :return: a tuple containing the new date, and the number of the field
                 that was actually incremented
        """
        i = 0
        values = {}
        while i < len(self.fields):
            field = self.fields[i]
            if not field.REAL:
                if i == fieldnum:
                    fieldnum -= 1
                    i -= 1
                else:
                    i += 1
                continue

            if i < fieldnum:
                values[field.name] = field.get_value(dateval)
                i += 1
            elif i > fieldnum:
                values[field.name] = field.get_min(dateval)
                i += 1
            else:
                value = field.get_value(dateval)
                maxval = field.get_max(dateval)
                if value == maxval:
                    fieldnum -= 1
                    i -= 1
                else:
                    values[field.name] = value + 1
                    i += 1

        return datetime(**values), fieldnum

    def _set_field_value(self, dateval, fieldnum, new_value):
        values = {}
        for i, field in enumerate(self.fields):
            if field.REAL:
                if i < fieldnum:
                    values[field.name] = field.get_value(dateval)
                elif i > fieldnum:
                    values[field.name] = field.get_min(dateval)
                else:
                    values[field.name] = new_value

        return datetime(**values)

    def get_next_fire_time(self, start_date):
        next_date = datetime_ceil(start_date)
        fieldnum = 0
        while 0 <= fieldnum < len(self.fields):
            field = self.fields[fieldnum]
            curr_value = field.get_value(next_date)
            next_value = field.get_next_value(next_date)

            if next_value is None:
                # No valid value was found
                next_date, fieldnum = self._increment_field_value(next_date,
                                                                  fieldnum - 1)
            elif next_value > curr_value:
                # A valid, but higher than the starting value, was found
                if field.REAL:
                    next_date = self._set_field_value(next_date, fieldnum,
                                                      next_value)
                    fieldnum += 1
                else:
                    next_date, fieldnum = self._increment_field_value(next_date,
                                                                      fieldnum)
            else:
                # A valid value was found, no changes necessary
                fieldnum += 1

        if fieldnum >= 0:
            return next_date

    def __repr__(self):
        field_reprs = ("%s='%s'" % (f.name, str(f)) for f in self.fields
                       if str(f) != '*')
        return '%s(%s)' % (self.__class__.__name__, ', '.join(field_reprs))


class DateTrigger(object):
    def __init__(self, run_date):
        self.run_date = convert_to_datetime(run_date)

    def get_next_fire_time(self, start_date):
        if self.run_date >= start_date:
            return self.run_date

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, repr(self.run_date))


class IntervalTrigger(object):
    def __init__(self, interval, repeat, start_date=None):
        if not isinstance(interval, timedelta):
            raise TypeError('interval must be a timedelta')
        if repeat < 0:
            raise ValueError('Illegal value for repeat; expected >= 0, '
                             'received %s' % repeat)

        self.interval = interval
        self.interval_length = timedelta_seconds(self.interval)
        if self.interval_length == 0:
            self.interval = timedelta(seconds=1)
            self.interval_length = 1
        self.repeat = repeat
        if start_date is None:
            self.first_fire_date = datetime.now() + self.interval
        else:
            self.first_fire_date = convert_to_datetime(start_date)
        self.first_fire_date -= timedelta(microseconds=\
                                          self.first_fire_date.microsecond)
        if repeat > 0:
            self.last_fire_date = self.first_fire_date + interval * (repeat - 1)
        else:
            self.last_fire_date = None

    def get_next_fire_time(self, start_date):
        if start_date < self.first_fire_date:
            return self.first_fire_date
        if self.last_fire_date and start_date > self.last_fire_date:
            return None
        timediff_seconds = timedelta_seconds(start_date - self.first_fire_date)
        next_interval_num = int(ceil(timediff_seconds / self.interval_length))
        return self.first_fire_date + self.interval * next_interval_num

    def __repr__(self):
        return "%s(interval=%s, repeat=%d, start_date=%s)" % (
            self.__class__.__name__, repr(self.interval), self.repeat,
            repr(self.first_fire_date))
