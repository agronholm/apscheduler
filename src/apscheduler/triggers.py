from datetime import datetime, timedelta
from math import ceil

from apscheduler.expressions import *
from apscheduler.util import *

__all__ = ('CronTrigger', 'DateTrigger', 'IntervalTrigger')
    

class CronTrigger(object):
    def __init__(self, year='*', month='*', day='*', day_of_week='*',
                 hour='*', minute='*', second='*'):
        self.fields = []
        self._compile_expressions(year, 'year')
        self._compile_expressions(month, 'month')
        self._compile_expressions(day, 'day')
        self._compile_expressions(day_of_week, 'day_of_week')
        self._compile_expressions(hour, 'hour')
        self._compile_expressions(minute, 'minute')
        self._compile_expressions(second, 'second')

    def _compile_expressions(self, exprs, fieldname):
        def compile_single(expr):
            compilers = [AllExpression, RangeExpression]
            if fieldname == 'day_of_week':
                compilers.append(DayOfWeekExpression)
            for compiler in compilers:
                m = compiler.value_re.match(expr)
                if m:
                    return compiler(**m.groupdict())
            raise ValueError('Unrecognized expression "%s" for field "%s"' %
                             (expr, fieldname))

        exprs = str(exprs).strip()
        if ',' in exprs:
            expr_list = exprs.split(',')
        else:
            expr_list = [exprs]
        compiled_expr_list = [compile_single(expr) for expr in expr_list]
        self.fields.append((fieldname, compiled_expr_list))
    
    def _set_field_value(self, date, fieldnum, new_value):
        """
        Sets the value of the designated field in the given datetime object.
        All less significant fields will be reset to their minimum values.
        @type date: datetime
        @type fieldnum: int
        @type new_value: int
        @rtype: datetime
        """

        # Fill in values for all date fields
        values = {}
        for i, field in enumerate(self.fields):
            fieldname = field[0]
            if not hasattr(date, fieldname):
                continue
            if i < fieldnum:
                values[fieldname] = getattr(date, fieldname)
            elif i > fieldnum:
                values[fieldname] = min_values[fieldname]
            else:
                values[fieldname] = new_value
        
        return datetime(**values)
    
    def _increment_field_value(self, date, fieldnum):
        """
        Increments the designated field and resets all less significant fields
        to their minimum values.
        @type date: datetime
        @type fieldnum: int
        @type amount: int
        @rtype: datetime
        """

        # If the given field is already at its maximum, then increment the
        # next most significant field
        fieldname = self.fields[fieldnum][0]
        value = getattr(date, fieldname)
        maxval = get_actual_maximum(date, fieldname)
        if value == maxval:
            return self._increment_field_value(date, fieldnum - 1)
        return self._set_field_value(date, fieldnum, value + 1)

    def get_next_fire_time(self, start_date):
        next_date = start_date
        fieldnum = 0
        while fieldnum < len(self.fields):
            fieldname, expr_list = self.fields[fieldnum]
            startval = get_date_field(next_date, fieldname)

            # Calculate the smallest suitable value for this field
            nextval = None
            for expr in expr_list:
                val = expr.get_next_value(next_date, fieldname)
                if nextval is not None:
                    nextval = min(val, nextval)
                else:
                    nextval = val
            
            if nextval is None or (fieldname == 'day_of_week' and nextval > startval):
                # No valid value was found for this field
                if fieldnum == 0:
                    # No valid values found for the year field, so give up
                    return None
                # Return to the previous field and look for the next valid value
                fieldnum -= 1
                next_date = self._increment_field_value(next_date, fieldnum)
            elif nextval > startval:
                # A valid value was found, but it was higher than the starting
                # value, so reset all less significant fields so as not to miss
                # any potential combinations
                next_date = self._set_field_value(next_date, fieldnum, nextval)
                fieldnum += 1
            else:
                # The field's value was accepted without modifications
                fieldnum += 1

        return next_date


class DateTrigger(object):
    def __init__(self, date):
        self.date = date
    
    def get_next_fire_time(self, start_date):
        if self.date >= start_date:
            return self.date


class IntervalTrigger(object):
    def __init__(self, interval, repeat, start_date=None):
        if not isinstance(interval, timedelta):
            raise TypeError('interval must be a timedelta')
        if start_date and not isinstance(start_date, datetime):
            raise TypeError('start_date must be a datetime')
        if repeat < 0:
            raise ValueError('Illegal value for repeat; expected >= 0, '
                             'received %s' % repeat)

        self.interval = interval
        self.interval_length = timedelta_seconds(self.interval)
        self.repeat = repeat
        if start_date is None:
            self.first_fire_date = datetime.now() + self.interval
        elif isinstance(start_date, datetime):
            self.first_fire_date = start_date
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
