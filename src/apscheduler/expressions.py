import re
from calendar import weekday, monthrange

from apscheduler.util import *


class AllExpression(object):
    value_re = re.compile(r'\*(?:/(?P<step>\d+))?$')

    def __init__(self, step=None):
        self.step = asint(step)
        if self.step == 0:
            raise ValueError('Increment must be higher than 0')

    def _get_minval(self, date, field):
        return min_values[field]
    
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


class RangeExpression(AllExpression):
    value_re = re.compile(r'(?P<first>\d+)(?:-(?P<last>\d+))?(?:/(?P<step>\d+))?$')

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


class DayOfWeekExpression(object):
    options = ['1st', '2nd', '3rd', '4th', '5th', 'last']
    weekdays = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']
    value_re = re.compile('(?P<option>%s) +(?P<weekday>(?:\d+|\w+))'
                          % '|'.join(options), re.IGNORECASE)

    def __init__(self, option_name, weekday_name):
        self.option_num = self.options.index(option_name.lower())
        self.weekday = self.weekdays.index(weekday_name.lower())

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