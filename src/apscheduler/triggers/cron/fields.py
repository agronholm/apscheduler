"""
Fields represent CronTrigger options which map to :class:`~datetime.datetime` fields.
"""

from __future__ import annotations

import re
from calendar import monthrange
from datetime import datetime
from typing import Any, ClassVar, Sequence

from .expressions import (
    WEEKDAYS,
    AllExpression,
    LastDayOfMonthExpression,
    MonthRangeExpression,
    RangeExpression,
    WeekdayPositionExpression,
    WeekdayRangeExpression,
    get_weekday_index,
)

MIN_VALUES = {
    "year": 1970,
    "month": 1,
    "day": 1,
    "week": 1,
    "day_of_week": 0,
    "hour": 0,
    "minute": 0,
    "second": 0,
}
MAX_VALUES = {
    "year": 9999,
    "month": 12,
    "day": 31,
    "week": 53,
    "day_of_week": 7,
    "hour": 23,
    "minute": 59,
    "second": 59,
}
DEFAULT_VALUES = {
    "year": "*",
    "month": 1,
    "day": 1,
    "week": "*",
    "day_of_week": "*",
    "hour": 0,
    "minute": 0,
    "second": 0,
}
SEPARATOR = re.compile(" *, *")


class BaseField:
    __slots__ = "name", "expressions"

    real: ClassVar[bool] = True
    compilers: ClassVar[Any] = (AllExpression, RangeExpression)

    def __init_subclass__(cls, real: bool = True, extra_compilers: Sequence = ()):
        cls.real = real
        if extra_compilers:
            cls.compilers += extra_compilers

    def __init__(self, name: str, exprs: int | str):
        self.name = name
        self.expressions: list = []
        for expr in SEPARATOR.split(str(exprs).strip()):
            self.append_expression(expr)

    def get_min(self, dateval: datetime) -> int:
        return MIN_VALUES[self.name]

    def get_max(self, dateval: datetime) -> int:
        return MAX_VALUES[self.name]

    def get_value(self, dateval: datetime) -> int:
        return getattr(dateval, self.name)

    def get_next_value(self, dateval: datetime) -> int | None:
        smallest = None
        for expr in self.expressions:
            value = expr.get_next_value(dateval, self)
            if smallest is None or (value is not None and value < smallest):
                smallest = value

        return smallest

    def append_expression(self, expr: str) -> None:
        for compiler in self.compilers:
            match = compiler.value_re.match(expr)
            if match:
                compiled_expr = compiler(**match.groupdict())

                try:
                    compiled_expr.validate_range(
                        self.name, MIN_VALUES[self.name], MAX_VALUES[self.name]
                    )
                except ValueError as exc:
                    raise ValueError(
                        f"Error validating expression {expr!r}: {exc}"
                    ) from exc

                self.expressions.append(compiled_expr)
                return

        raise ValueError(f"Unrecognized expression {expr!r} for field {self.name!r}")

    def __str__(self):
        expr_strings = (str(e) for e in self.expressions)
        return ",".join(expr_strings)


class WeekField(BaseField, real=False):
    __slots__ = ()

    def get_value(self, dateval: datetime) -> int:
        return dateval.isocalendar()[1]


class DayOfMonthField(
    BaseField, extra_compilers=(WeekdayPositionExpression, LastDayOfMonthExpression)
):
    __slots__ = ()

    def get_max(self, dateval: datetime) -> int:
        return monthrange(dateval.year, dateval.month)[1]


class DayOfWeekField(BaseField, real=False, extra_compilers=(WeekdayRangeExpression,)):
    __slots__ = ()

    def append_expression(self, expr: str) -> None:
        # Convert numeric weekday expressions into textual ones
        match = RangeExpression.value_re.match(expr)
        if match:
            groups = match.groups()
            first = int(groups[0]) - 1
            first = 6 if first < 0 else first
            if groups[1]:
                last = int(groups[1]) - 1
                last = 6 if last < 0 else last
            else:
                last = first

            expr = f"{WEEKDAYS[first]}-{WEEKDAYS[last]}"

        # For expressions like Sun-Tue or Sat-Mon, add two expressions that together
        # cover the expected weekdays
        match = WeekdayRangeExpression.value_re.match(expr)
        if match and match.groups()[1]:
            groups = match.groups()
            first_index = get_weekday_index(groups[0])
            last_index = get_weekday_index(groups[1])
            if first_index > last_index:
                super().append_expression(f"{WEEKDAYS[0]}-{groups[1]}")
                super().append_expression(f"{groups[0]}-{WEEKDAYS[-1]}")
                return

        super().append_expression(expr)

    def get_value(self, dateval: datetime) -> int:
        return dateval.weekday()


class MonthField(BaseField, extra_compilers=(MonthRangeExpression,)):
    __slots__ = ()
