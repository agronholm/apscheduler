"""
Fields represent :class:`~apscheduler.triggers.CronTrigger` options which map
to :class:`~datetime.datetime` fields.
"""
from calendar import monthrange

from apscheduler.expressions import *

__all__ = ('BaseField', 'WeekField', 'DayOfMonthField', 'DayOfWeekField')

MIN_VALUES = {'year': 1970, 'month': 1, 'day': 1, 'week': 1,
              'day_of_week': 0, 'hour': 0, 'minute': 0, 'second': 0}
MAX_VALUES = {'year': 2 ** 63, 'month': 12, 'day:': 31, 'week': 53,
              'day_of_week': 6, 'hour': 23, 'minute': 59, 'second': 59}

class BaseField(object):
    REAL = True
    COMPILERS = [AllExpression, RangeExpression]

    def __init__(self, name, exprs):
        self.name = name
        self.compile_expressions(exprs)

    def get_min(self, dateval):
        return MIN_VALUES[self.name]

    def get_max(self, dateval):
        return MAX_VALUES[self.name]

    def get_value(self, dateval):
        return getattr(dateval, self.name)

    def get_next_value(self, dateval):
        smallest = None
        for expr in self.expressions:
            value = expr.get_next_value(dateval, self)
            if smallest is None or (value is not None and value < smallest):
                smallest = value

        return smallest

    def compile_expressions(self, exprs):
        self.expressions = []

        # Split a comma-separated expression list, if any
        exprs = str(exprs).strip()
        if ',' in exprs:
            for expr in exprs.split(','):
                self.compile_expression(expr)
        else:
            self.compile_expression(exprs)

    def compile_expression(self, expr):
        for compiler in self.COMPILERS:
            match = compiler.value_re.match(expr)
            if match:
                compiled_expr = compiler(**match.groupdict())
                self.expressions.append(compiled_expr)
                return

        raise ValueError('Unrecognized expression "%s" for field "%s"' %
                         (expr, self.name))

    def __str__(self):
        expr_strings = (str(e) for e in self.expressions)
        return ','.join(expr_strings)

    def __repr__(self):
        return "%s('%s', '%s')" % (self.__class__.__name__, self.name,
                                   str(self))


class WeekField(BaseField):
    REAL = False

    def get_value(self, dateval):
        return dateval.isocalendar()[1]


class DayOfMonthField(BaseField):
    COMPILERS = BaseField.COMPILERS + [WeekdayPositionExpression]

    def get_max(self, dateval):
        return monthrange(dateval.year, dateval.month)[1]


class DayOfWeekField(BaseField):
    REAL = False
    COMPILERS = BaseField.COMPILERS + [WeekdayRangeExpression]

    def get_value(self, dateval):
        return dateval.weekday()
