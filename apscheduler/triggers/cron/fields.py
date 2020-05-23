"""Fields represent CronTrigger options which map to :class:`~datetime.datetime` fields."""

import re
from calendar import monthrange
from datetime import datetime
from typing import Sequence, ClassVar, Any, Union, Optional

from .expressions import (
    AllExpression, RangeExpression, WeekdayPositionExpression, LastDayOfMonthExpression,
    WeekdayRangeExpression, MonthRangeExpression)

MIN_VALUES = {'year': 1970, 'month': 1, 'day': 1, 'week': 1, 'day_of_week': 0, 'hour': 0,
              'minute': 0, 'second': 0}
MAX_VALUES = {'year': 9999, 'month': 12, 'day': 31, 'week': 53, 'day_of_week': 6, 'hour': 23,
              'minute': 59, 'second': 59}
DEFAULT_VALUES = {'year': '*', 'month': 1, 'day': 1, 'week': '*', 'day_of_week': '*', 'hour': 0,
                  'minute': 0, 'second': 0}
SEPARATOR = re.compile(' *, *')


class BaseField:
    __slots__ = 'name', 'expressions'

    real: ClassVar[bool] = True
    compilers: ClassVar[Any] = (AllExpression, RangeExpression)

    def __init_subclass__(cls, real: bool = True, extra_compilers: Sequence = ()):
        cls.real = real
        if extra_compilers:
            cls.compilers += extra_compilers

    def __init__(self, name: str, exprs: Union[int, str]):
        self.name = name
        self.expressions = [self.compile_expression(expr)
                            for expr in SEPARATOR.split(str(exprs).strip())]

    def get_min(self, dateval: datetime) -> int:
        return MIN_VALUES[self.name]

    def get_max(self, dateval: datetime) -> int:
        return MAX_VALUES[self.name]

    def get_value(self, dateval: datetime) -> int:
        return getattr(dateval, self.name)

    def get_next_value(self, dateval: datetime) -> Optional[int]:
        smallest = None
        for expr in self.expressions:
            value = expr.get_next_value(dateval, self)
            if smallest is None or (value is not None and value < smallest):
                smallest = value

        return smallest

    def compile_expression(self, expr: str):
        for compiler in self.compilers:
            match = compiler.value_re.match(expr)
            if match:
                compiled_expr = compiler(**match.groupdict())

                try:
                    compiled_expr.validate_range(self.name, MIN_VALUES[self.name],
                                                 MAX_VALUES[self.name])
                except ValueError as exc:
                    raise ValueError(f'Error validating expression {expr!r}: {exc}') from exc

                return compiled_expr

        raise ValueError(f'Unrecognized expression {expr!r} for field {self.name!r}')

    def __str__(self):
        expr_strings = (str(e) for e in self.expressions)
        return ','.join(expr_strings)


class WeekField(BaseField, real=False):
    __slots__ = ()

    def get_value(self, dateval: datetime) -> int:
        return dateval.isocalendar()[1]


class DayOfMonthField(BaseField,
                      extra_compilers=(WeekdayPositionExpression, LastDayOfMonthExpression)):
    __slots__ = ()

    def get_max(self, dateval: datetime) -> int:
        return monthrange(dateval.year, dateval.month)[1]


class DayOfWeekField(BaseField, real=False, extra_compilers=(WeekdayRangeExpression,)):
    __slots__ = ()

    def get_value(self, dateval: datetime) -> int:
        return dateval.weekday()


class MonthField(BaseField, extra_compilers=(MonthRangeExpression,)):
    __slots__ = ()
