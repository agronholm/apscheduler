"""This module contains the expressions applicable for CronTrigger's fields."""
from __future__ import annotations

import re
from calendar import monthrange
from datetime import datetime

from ..._validators import as_int

WEEKDAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
MONTHS = [
    "jan",
    "feb",
    "mar",
    "apr",
    "may",
    "jun",
    "jul",
    "aug",
    "sep",
    "oct",
    "nov",
    "dec",
]


def get_weekday_index(weekday: str) -> int:
    try:
        return WEEKDAYS.index(weekday.lower())
    except ValueError:
        raise ValueError(f"Invalid weekday name {weekday!r}") from None


class AllExpression:
    __slots__ = "step"

    value_re = re.compile(r"\*(?:/(?P<step>\d+))?$")

    def __init__(self, step: str | int | None = None):
        self.step = as_int(step)
        if self.step == 0:
            raise ValueError("Step must be higher than 0")

    def validate_range(self, field_name: str, min_value: int, max_value: int) -> None:
        value_range = max_value - min_value
        if self.step and self.step > value_range:
            raise ValueError(
                f"the step value ({self.step}) is higher than the total range of the "
                f"expression ({value_range})"
            )

    def get_next_value(self, dateval: datetime, field) -> int | None:
        start = field.get_value(dateval)
        minval = field.get_min(dateval)
        maxval = field.get_max(dateval)
        start = max(start, minval)

        if not self.step:
            nextval = start
        else:
            distance_to_next = (self.step - (start - minval)) % self.step
            nextval = start + distance_to_next

        return nextval if nextval <= maxval else None

    def __str__(self):
        return f"*/{self.step}" if self.step else "*"


class RangeExpression(AllExpression):
    __slots__ = "first", "last"

    value_re = re.compile(r"(?P<first>\d+)(?:-(?P<last>\d+))?(?:/(?P<step>\d+))?$")

    def __init__(
        self,
        first: str | int,
        last: str | int | None = None,
        step: str | int | None = None,
    ):
        super().__init__(step)
        self.first = as_int(first)
        self.last = as_int(last)

        if self.last is None and self.step is None:
            self.last = self.first
        if self.last is not None and self.first > self.last:
            raise ValueError(
                "The minimum value in a range must not be higher than the maximum"
            )

    def validate_range(self, field_name: str, min_value: int, max_value: int) -> None:
        super().validate_range(field_name, min_value, max_value)
        if self.first < min_value:
            raise ValueError(
                f"the first value ({self.first}) is lower than the minimum value "
                f"({min_value})"
            )
        if self.last is not None and self.last > max_value:
            raise ValueError(
                f"the last value ({self.last}) is higher than the maximum value "
                f"({max_value})"
            )
        value_range = (self.last or max_value) - self.first
        if self.step and self.step > value_range:
            raise ValueError(
                f"the step value ({self.step}) is higher than the total range of the "
                f"expression ({value_range})"
            )

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

    def __str__(self):
        if self.last != self.first and self.last is not None:
            rangeval = f"{self.first}-{self.last}"
        else:
            rangeval = str(self.first)

        if self.step:
            return f"{rangeval}/{self.step}"

        return rangeval


class MonthRangeExpression(RangeExpression):
    __slots__ = ()

    value_re = re.compile(r"(?P<first>[a-z]+)(?:-(?P<last>[a-z]+))?", re.IGNORECASE)

    def __init__(self, first, last=None):
        try:
            first_num = MONTHS.index(first.lower()) + 1
        except ValueError:
            raise ValueError(f"Invalid month name {first!r}") from None

        if last:
            try:
                last_num = MONTHS.index(last.lower()) + 1
            except ValueError:
                raise ValueError(f"Invalid month name {last!r}") from None
        else:
            last_num = None

        super().__init__(first_num, last_num)

    def __str__(self):
        if self.last != self.first and self.last is not None:
            return f"{MONTHS[self.first - 1]}-{MONTHS[self.last - 1]}"

        return MONTHS[self.first - 1]


class WeekdayRangeExpression(RangeExpression):
    __slots__ = ()

    value_re = re.compile(r"(?P<first>[a-z]+)(?:-(?P<last>[a-z]+))?", re.IGNORECASE)

    def __init__(self, first: str, last: str | None = None):
        first_num = get_weekday_index(first)
        last_num = get_weekday_index(last) if last else None
        super().__init__(first_num, last_num)

    def __str__(self):
        if self.last != self.first and self.last is not None:
            return f"{WEEKDAYS[self.first]}-{WEEKDAYS[self.last]}"

        return WEEKDAYS[self.first]


class WeekdayPositionExpression(AllExpression):
    __slots__ = "option_num", "weekday"

    options = ["1st", "2nd", "3rd", "4th", "5th", "last"]
    value_re = re.compile(
        r"(?P<option_name>%s) +(?P<weekday_name>(?:\d+|\w+))" % "|".join(options),
        re.IGNORECASE,
    )

    def __init__(self, option_name: str, weekday_name: str):
        super().__init__(None)
        self.option_num = self.options.index(option_name.lower())
        try:
            self.weekday = WEEKDAYS.index(weekday_name.lower())
        except ValueError:
            raise ValueError(f"Invalid weekday name {weekday_name!r}") from None

    def get_next_value(self, dateval: datetime, field) -> int | None:
        # Figure out the weekday of the month's first day and the number of days in that
        # month
        first_day_wday, last_day = monthrange(dateval.year, dateval.month)

        # Calculate which day of the month is the first of the target weekdays
        first_hit_day = self.weekday - first_day_wday + 1
        if first_hit_day <= 0:
            first_hit_day += 7

        # Calculate what day of the month the target weekday would be
        if self.option_num < 5:
            target_day = first_hit_day + self.option_num * 7
        else:
            target_day = first_hit_day + ((last_day - first_hit_day) // 7) * 7

        if last_day >= target_day >= dateval.day:
            return target_day
        else:
            return None

    def __str__(self):
        return f"{self.options[self.option_num]} {WEEKDAYS[self.weekday]}"


class LastDayOfMonthExpression(AllExpression):
    __slots__ = ()

    value_re = re.compile(r"last", re.IGNORECASE)

    def __init__(self):
        super().__init__(None)

    def get_next_value(self, dateval: datetime, field):
        return monthrange(dateval.year, dateval.month)[1]

    def __str__(self):
        return "last"
