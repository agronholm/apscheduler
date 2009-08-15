"""
This module contains the expressions applicable for CronTrigger's fields.
"""
import re
from calendar import weekday, monthrange

from apscheduler.util import *

__all__ = ['AllExpression', 'RangeExpression', 'WeekdayRangeExpression',
           'WeekdayPositionExpression']

WEEKDAYS = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']


class AllExpression(object):
    value_re = re.compile(r'\*(?:/(?P<step>\d+))?$')

    def __init__(self, step=None):
        self.step = asint(step)
        if self.step == 0:
            raise ValueError('Increment must be higher than 0')

    def _get_minval(self, date, field):
        return MIN_VALUES[field]

    def _get_maxval(self, date, field):
        return get_actual_maximum(date, field)

    def get_next_value(self, date, fieldname):
        # day_of_week is not an attribute of datetime
        start = get_date_field(date, fieldname)
        minval = self._get_minval(date, fieldname)
        maxval = self._get_maxval(date, fieldname)
        start = max(start, minval)
        if not self.step:
            next = start
        else:
            distance_to_next = (self.step - (start - minval)) % self.step
            next = start + distance_to_next

        if next <= maxval:
            return next

    def __str__(self):
        if self.step:
            return '*/%d' % self.step
        return '*'


class RangeExpression(AllExpression):
    value_re = re.compile(
        r'(?P<first>\d+)(?:-(?P<last>\d+))?(?:/(?P<step>\d+))?$')

    def __init__(self, first, last=None, step=None):
        AllExpression.__init__(self, step)
        first = asint(first)
        last = asint(last)
        if last is None and step is None:
            last = first
        if last is not None and first > last:
            raise ValueError('The minimum value in a range must not be '
                             'higher than the maximum')
        self.first = first
        self.last = last

    def _get_minval(self, date, field):
        minval = AllExpression._get_minval(self, date, field)
        return max(minval, self.first)

    def _get_maxval(self, date, field):
        maxval = AllExpression._get_maxval(self, date, field)
        if self.last is not None:
            return min(maxval, self.last)
        return maxval

    def __str__(self):
        if self.last != self.first:
            if self.step:
                return '%d-%d/%d' % (self.first, self.last, self.step)
            else:
                return '%d-%d' % (self.first, self.last)
        return str(self.first)


class WeekdayRangeExpression(RangeExpression):
    value_re = re.compile(r'(?P<first>[a-z]+)(?:-(?P<last>[a-z]+))',
                          re.IGNORECASE)

    def __init__(self, first, last=None):
        try:
            first_num = WEEKDAYS.index(first.lower())
        except ValueError:
            raise ValueError('Invalid weekday name "%s"' % first)

        if last:
            try:
                last_num = WEEKDAYS.index(last.lower())
            except ValueError:
                raise ValueError('Invalid weekday name "%s"' % last)
        else:
            last_num = None

        RangeExpression.__init__(self, first_num, last_num)

    def __str__(self):
        if self.last != self.first:
            return '%s-%s' % (WEEKDAYS[self.first], WEEKDAYS[self.last])
        return WEEKDAYS[self.first]


class WeekdayPositionExpression(object):
    options = ['1st', '2nd', '3rd', '4th', '5th', 'last']
    value_re = re.compile(r'(?P<option>%s) +(?P<weekday>(?:\d+|\w+))'
                          % '|'.join(options), re.IGNORECASE)

    def __init__(self, option_name, weekday_name):
        try:
            self.option_num = self.options.index(option_name.lower())
        except ValueError:
            raise ValueError('Invalid weekday position "%s"' % option_name)

        try:
            self.weekday = WEEKDAYS.index(weekday_name.lower())
        except ValueError:
            raise ValueError('Invalid weekday name "%s"' % weekday_name)

    def get_next_value(self, date, field):
        hits = 0
        last_hit = None
        wday, last_day = monthrange(date.year, date.month)
        for day in range(date.day, last_day+1):
            wday = weekday(date.year, date.month, day)
            if wday == self.weekday:
                if hits == self.option_num:
                    return day
                hits += 1
                last_hit = day
        if self.option_num == 5:
            return last_hit

    def __str__(self):
        return '%s %s' % (self.options[self.option_num],
                          WEEKDAYS[self.weekday])
