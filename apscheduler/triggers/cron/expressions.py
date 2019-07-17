"""This module contains the expressions applicable for CronTrigger's fields."""

from calendar import monthrange
import re

from apscheduler.util import asint

__all__ = ('AllExpression', 'RangeExpression', 'WeekdayRangeExpression',
           'WeekdayPositionExpression', 'LastDayOfMonthExpression')


WEEKDAYS = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']
WEEKDAYS_POSIX = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat']
MONTHS = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']


def weekdays(standard):
    return WEEKDAYS_POSIX if standard == 'POSIX.1-2017' else WEEKDAYS


class AllExpression(object):
    value_re = re.compile(r'\*(?:/(?P<step>\d+))?$')

    def __init__(self, step=None, standard=None):
        self.standard = standard
        self.step = asint(step)
        if self.step == 0:
            raise ValueError('Increment must be higher than 0')

    def validate_range(self, field_name):
        from apscheduler.triggers.cron.fields import MIN_VALUES, MAX_VALUES

        value_range = MAX_VALUES[field_name] - MIN_VALUES[field_name]
        if self.step and self.step > value_range:
            raise ValueError('the step value ({}) is higher than the total range of the '
                             'expression ({})'.format(self.step, value_range))

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

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.step == other.step

    def __str__(self):
        if self.step:
            return '*/%d' % self.step
        return '*'

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, self.step)


class RangeExpression(AllExpression):
    value_re = re.compile(
        r'(?P<first>\d+)(?:-(?P<last>\d+))?(?:/(?P<step>\d+))?$')

    def __init__(self, first, last=None, step=None, standard=None):
        super(RangeExpression, self).__init__(step, standard)
        first = asint(first)
        last = asint(last)
        if last is None and step is None:
            last = first
        if last is not None and first > last:
            raise ValueError('The minimum value in a range must not be higher than the maximum')
        self.first = first
        self.last = last

    def validate_range(self, field_name):
        from apscheduler.triggers.cron.fields import MIN_VALUES, MAX_VALUES

        super(RangeExpression, self).validate_range(field_name)
        if self.first < MIN_VALUES[field_name]:
            raise ValueError('the first value ({}) is lower than the minimum value ({})'
                             .format(self.first, MIN_VALUES[field_name]))
        if self.last is not None and self.last > MAX_VALUES[field_name]:
            raise ValueError('the last value ({}) is higher than the maximum value ({})'
                             .format(self.last, MAX_VALUES[field_name]))
        value_range = (self.last or MAX_VALUES[field_name]) - self.first
        if self.step and self.step > value_range:
            raise ValueError('the step value ({}) is higher than the total range of the '
                             'expression ({})'.format(self.step, value_range))

    def get_next_value(self, date, field):
        startval = field.get_value(date)
        minval = field.get_min(date)
        maxval = field.get_max(date)

        # Apply range limits
        minval = max(minval, self.first)
        maxval = min(maxval, self.last) if self.last is not None else maxval
        nextval = max(minval, startval)

        # Apply the step if defined
        if self.step:
            distance_to_next = (self.step - (nextval - minval)) % self.step
            nextval += distance_to_next

        return nextval if nextval <= maxval else None

    def __eq__(self, other):
        return (isinstance(other, self.__class__) and self.first == other.first and
                self.last == other.last)

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


class MonthRangeExpression(RangeExpression):
    value_re = re.compile(
        r'(?P<first>[a-z]+)(?:-(?P<last>[a-z]+))?(?:/(?P<step>\d+))?$',
        re.IGNORECASE
    )

    def __init__(self, first, last=None, step=None, standard=None):
        try:
            first_num = MONTHS.index(first.lower()) + 1
        except ValueError:
            raise ValueError('Invalid month name "%s"' % first)

        if last:
            try:
                last_num = MONTHS.index(last.lower()) + 1
            except ValueError:
                raise ValueError('Invalid month name "%s"' % last)
        else:
            last_num = None

        super(MonthRangeExpression, self).__init__(first_num, last_num, step, standard=standard)

    def __str__(self):
        if self.last != self.first and self.last is not None:
            range = '%s-%s' % (MONTHS[self.first - 1], MONTHS[self.last - 1])
        else:
            range = MONTHS[self.first - 1]

        if self.step:
            return '%s/%d' % (range, self.step)
        return range

    def __repr__(self):
        args = ["'%s'" % MONTHS[self.first]]
        if self.last != self.first and self.last is not None:
            args.append("'%s'" % MONTHS[self.last - 1])
        if self.step:
            args.append(str(self.step))
        return "%s(%s)" % (self.__class__.__name__, ', '.join(args))


class WeekdayRangeExpression(RangeExpression):
    value_re = re.compile(
        r'(?P<first>[a-z]+)(?:-(?P<last>[a-z]+))?(?:/(?P<step>\d+))?$',
        re.IGNORECASE
    )

    def __init__(self, first, last=None, step=None, standard=None):
        self.weekdays = weekdays(standard)
        try:
            first_num = self.weekdays.index(first.lower())
        except ValueError:
            raise ValueError('Invalid weekday name "%s"' % first)

        if last:
            try:
                last_num = self.weekdays.index(last.lower())
            except ValueError:
                raise ValueError('Invalid weekday name "%s"' % last)
        else:
            last_num = None

        super(WeekdayRangeExpression, self).__init__(first_num, last_num, step, standard=standard)

    def __str__(self):
        if self.last != self.first and self.last is not None:
            range = '%s-%s' % (self.weekdays[self.first], self.weekdays[self.last])
        else:
            range = self.weekdays[self.first]

        if self.step:
            return '%s/%d' % (range, self.step)
        return range

    def __repr__(self):
        args = ["'%s'" % self.weekdays[self.first]]
        if self.last != self.first and self.last is not None:
            args.append("'%s'" % self.weekdays[self.last])
        if self.step:
            args.append(str(self.step))
        return "%s(%s)" % (self.__class__.__name__, ', '.join(args))


class WeekdayPositionExpression(AllExpression):
    options = ['1st', '2nd', '3rd', '4th', '5th', 'last']
    value_re = re.compile(r'(:?%s)$' %
                          '|'.join([
                              r'(:?(?P<option_name>%s) +(?P<weekday_name>(?:\d+|\w+)))' %
                              '|'.join(options),
                              r'(:?(?P<weekday_name2>(?:\d+|\w+))#(?P<option_name2>[1-5L]))'
                          ]), re.IGNORECASE)

    def __init__(self, option_name=None, weekday_name=None,
                 option_name2=None, weekday_name2=None, standard=None):
        super(WeekdayPositionExpression, self).__init__(None, standard=standard)

        if option_name2:
            weekday_name = weekday_name2
            self.option_num = 5 if option_name2.upper() == 'L' else int(option_name2) - 1
            if self.option_num > 5 or self.option_num < 0:
                raise ValueError('Invalid weekday position "%s"' % option_name2)
        else:
            try:
                self.option_num = self.options.index(option_name.lower())
            except ValueError:
                raise ValueError('Invalid weekday position "%s"' % option_name)

        self.weekdays = weekdays(standard)
        if len(weekday_name) == 1:
            self.weekday = int(weekday_name)
            if self.weekday > 6:
                raise ValueError('Invalid weekday "%s"' % weekday_name)
        else:
            try:
                self.weekday = self.weekdays.index(weekday_name.lower())
            except ValueError:
                raise ValueError('Invalid weekday name "%s"' % weekday_name)

        print(locals(), self.value_re)

    def get_next_value(self, date, field):
        # Figure out the weekday of the month's first day and the number of days in that month
        first_day_wday, last_day = monthrange(date.year, date.month)

        # Calculate which day of the month is the first of the target weekdays
        first_hit_day = self.weekday - first_day_wday + 1
        if first_hit_day <= 0:
            first_hit_day += 7

        # Calculate what day of the month the target weekday would be
        if self.option_num < 5:
            target_day = first_hit_day + self.option_num * 7
        else:
            target_day = first_hit_day + ((last_day - first_hit_day) // 7) * 7

        if target_day <= last_day and target_day >= date.day:
            return target_day

    def __eq__(self, other):
        return (super(WeekdayPositionExpression, self).__eq__(other) and
                self.option_num == other.option_num and self.weekday == other.weekday)

    def __str__(self):
        return '%s %s' % (self.options[self.option_num], self.weekdays[self.weekday])

    def __repr__(self):
        return "%s('%s', '%s')" % (self.__class__.__name__, self.options[self.option_num],
                                   self.weekdays[self.weekday])


class LastDayOfMonthExpression(AllExpression):
    value_re = re.compile(r'last$', re.IGNORECASE)

    def __init__(self, standard=None):
        super(LastDayOfMonthExpression, self).__init__(None, standard=standard)

    def get_next_value(self, date, field):
        return monthrange(date.year, date.month)[1]

    def __str__(self):
        return 'last'

    def __repr__(self):
        return "%s()" % self.__class__.__name__
