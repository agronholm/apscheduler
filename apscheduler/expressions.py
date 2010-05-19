"""
This module contains the expressions applicable for CronTrigger's fields.
"""
from calendar import monthrange
import re

from apscheduler.util import asint

__all__ = ('AllExpression', 'RangeExpression', 'WeekdayRangeExpression',
           'WeekdayPositionExpression')

WEEKDAYS = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']


class AllExpression(object):
    value_re = re.compile(r'\*(?:/(?P<step>\d+))?$')

    def __init__(self, step=None):
        self.step = asint(step)
        if self.step == 0:
            raise ValueError('Increment must be higher than 0')

    def get_next_value(self, date, field):
        start = field.get_value(date)
        minval = field.get_min(date)
        maxval = field.get_max(date)
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

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, self.step)


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

    def get_next_value(self, date, field):
        start = field.get_value(date)
        minval = field.get_min(date)
        maxval = field.get_max(date)
        
        # Apply range limits
        minval = max(minval, self.first)
        if self.last is not None:
            maxval = min(maxval, self.last)
        start = max(start, minval)

        if not self.step:
            next = start
        else:
            distance_to_next = (self.step - (start - minval)) % self.step
            next = start + distance_to_next

        if next <= maxval:
            return next

    def __str__(self):
        if self.last != self.first and self.last is not None:
            range = '%d-%d' % (self.first, self.last)
        else:
            range = str(self.first)
        
        if self.step:
            return '%s/%d' % (range, self.step)
        return range

    def __repr__(self):
        args = [str(self.first)]
        if self.last != self.first and self.last is not None or self.step:
            args.append(str(self.last))
        if self.step:
            args.append(str(self.step))
        return "%s(%s)" % (self.__class__.__name__, ', '.join(args))


class WeekdayRangeExpression(RangeExpression):
    value_re = re.compile(r'(?P<first>[a-z]+)(?:-(?P<last>[a-z]+))?',
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
        if self.last != self.first and self.last is not None:
            return '%s-%s' % (WEEKDAYS[self.first], WEEKDAYS[self.last])
        return WEEKDAYS[self.first]

    def __repr__(self):
        args = ["'%s'" % WEEKDAYS[self.first]]
        if self.last != self.first and self.last is not None:
            args.append("'%s'" % WEEKDAYS[self.last])
        return "%s(%s)" % (self.__class__.__name__, ', '.join(args))


class WeekdayPositionExpression(AllExpression):
    options = ['1st', '2nd', '3rd', '4th', '5th', 'last']
    value_re = re.compile(r'(?P<option_name>%s) +(?P<weekday_name>(?:\d+|\w+))'
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
        # Figure out the weekday of the month's first day and the number
        # of days in that month
        first_day_wday, last_day = monthrange(date.year, date.month)

        # Calculate which day of the month is the first of the target weekdays
        first_hit_day = self.weekday - first_day_wday + 1
        if first_hit_day <= 0:
            first_hit_day += 7

        # Calculate what day of the month the target weekday would be
        if self.option_num < 5:
            target_day = first_hit_day + self.option_num * 7
        else:
            target_day = first_hit_day + ((last_day - first_hit_day) / 7) * 7

        if target_day <= last_day and target_day >= date.day:
            return target_day

    def __str__(self):
        return '%s %s' % (self.options[self.option_num],
                          WEEKDAYS[self.weekday])

    def __repr__(self):
        return "%s('%s', '%s')" % (self.__class__.__name__,
                                   self.options[self.option_num],
                                   WEEKDAYS[self.weekday])
