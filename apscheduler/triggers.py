"""
Triggers determine the times when a job should be executed.
"""
from datetime import datetime, timedelta
from math import ceil

from apscheduler.expressions import *
from apscheduler.util import *

__all__ = ('CronTrigger', 'DateTrigger', 'IntervalTrigger')


class CronTrigger(object):
    def __init__(self, year='*', month='*', day='*', week='*',
                 day_of_week='*', hour='*', minute='*', second='*'):
        self.fields = []
        self._compile_expressions(year, 'year')
        self._compile_expressions(month, 'month')
        self._compile_expressions(day, 'day')
        self._compile_expressions(week, 'week')
        self._compile_expressions(day_of_week, 'day_of_week')
        self._compile_expressions(hour, 'hour')
        self._compile_expressions(minute, 'minute')
        self._compile_expressions(second, 'second')

    def _compile_expressions(self, exprs, fieldname):
        def compile_single(expr):
            compilers = [AllExpression, RangeExpression]
            if fieldname == 'day_of_week':
                compilers.append(WeekdayRangeExpression)
                compilers.append(WeekdayPositionExpression)
            for compiler in compilers:
                match = compiler.value_re.match(expr)
                if match:
                    return compiler(**match.groupdict())
            raise ValueError('Unrecognized expression "%s" for field "%s"' %
                             (expr, fieldname))

        exprs = str(exprs).strip()
        if ',' in exprs:
            expr_list = exprs.split(',')
        else:
            expr_list = [exprs]
        compiled_expr_list = [compile_single(expr) for expr in expr_list]
        self.fields.append((fieldname, compiled_expr_list))

    def _set_field_value(self, dateval, fieldnum, new_value):
        """
        Sets the value of the designated field in the given datetime object.
        All less significant fields will be reset to their minimum values.

        :type dateval: datetime
        :type fieldnum: int
        :type new_value: int
        :rtype: datetime
        """

        # Fill in values for all date fields
        values = {}
        for i, field in enumerate(self.fields):
            fieldname = field[0]
            if not hasattr(dateval, fieldname):
                continue
            if i < fieldnum:
                values[fieldname] = getattr(dateval, fieldname)
            elif i > fieldnum:
                values[fieldname] = MIN_VALUES[fieldname]
            else:
                values[fieldname] = new_value

        return datetime(**values)

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

        # If the given field is already at its maximum, or it has no counterpart
        # in a datetime object, then increment the next most significant field
        fieldname = self.fields[fieldnum][0]
        value = getattr(dateval, fieldname, None)
        if value is not None:
            maxval = get_actual_maximum(dateval, fieldname)
            if value < maxval:
                dateval = self._set_field_value(dateval, fieldnum, value + 1)
                return (dateval, fieldnum)
        return self._increment_field_value(dateval, fieldnum - 1)

    def get_next_fire_time(self, start_date):
        next_date = datetime_ceil(start_date)
        fieldnum = 0
        while fieldnum < len(self.fields):
            fieldname, expr_list = self.fields[fieldnum]
            startval = get_date_field(next_date, fieldname)

            # Calculate the smallest suitable value for this field
            nextval = None
            for expr in expr_list:
                val = expr.get_next_value(next_date, fieldname)
                if val is not None:
                    if nextval is not None:
                        nextval = min(val, nextval)
                    else:
                        nextval = val

            if nextval is None or (nextval > startval and not
                                   hasattr(datetime, fieldname)):
                # Either no valid value was found for this field,
                # or this field is a computed field and the current
                # value was not acceptable
                if fieldnum == 0:
                    # No valid values found for the first field, so give up
                    return None
                # Return to the previous field and look for the
                # next valid value
                next_date, fieldnum = self._increment_field_value(next_date,
                                                                  fieldnum)
            elif nextval > startval:
                # A valid value was found, but it was higher than the starting
                # value, so reset all less significant fields so as not to miss
                # any potential combinations
                next_date = self._set_field_value(next_date, fieldnum, nextval)
                fieldnum += 1
            else:
                # The field's value was accepted without modifications
                fieldnum += 1

        next_date = next_date - timedelta(microseconds=next_date.microsecond)
        return next_date


class DateTrigger(object):
    def __init__(self, run_date):
        self.run_date = convert_to_datetime(run_date)

    def get_next_fire_time(self, start_date):
        if self.run_date >= start_date:
            return self.run_date


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
            self.last_fire_date = self.first_fire_date + interval * (repeat-1)
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
