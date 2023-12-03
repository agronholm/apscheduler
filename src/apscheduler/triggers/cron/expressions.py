"""This module contains the expressions applicable for CronTrigger's fields."""
from __future__ import annotations

import re
from calendar import monthrange
from datetime import datetime
from typing import TYPE_CHECKING, ClassVar, Pattern

import attrs
from attr.validators import instance_of, optional

from ..._converters import as_int
from ..._validators import non_negative_number, positive_number

if TYPE_CHECKING:
    from .fields import BaseField

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


@attrs.define(slots=True)
class AllExpression:
    value_re: ClassVar[Pattern] = re.compile(r"\*(?:/(?P<step>\d+))?$")

    step: int | None = attrs.field(
        converter=as_int,
        validator=optional([instance_of(int), positive_number]),
        default=None,
    )

    def validate_range(self, field_name: str, min_value: int, max_value: int) -> None:
        value_range = max_value - min_value
        if self.step and self.step > value_range:
            raise ValueError(
                f"the step value ({self.step}) is higher than the total range of the "
                f"expression ({value_range})"
            )

    def get_next_value(self, dateval: datetime, field: BaseField) -> int | None:
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

    def __str__(self) -> str:
        return f"*/{self.step}" if self.step else "*"


@attrs.define(kw_only=True, slots=True)
class RangeExpression(AllExpression):
    value_re: ClassVar[Pattern] = re.compile(
        r"(?P<first>\d+)(?:-(?P<last>\d+))?(?:/(?P<step>\d+))?$"
    )

    first: int = attrs.field(
        converter=as_int, validator=[instance_of(int), non_negative_number]
    )
    last: int = attrs.field(
        converter=as_int,
        validator=optional([instance_of(int), non_negative_number]),
        default=None,
    )

    def __attrs_post_init__(self) -> None:
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

    def get_next_value(self, dateval: datetime, field: BaseField) -> int | None:
        startval = field.get_value(dateval)
        minval = field.get_min(dateval)
        maxval = field.get_max(dateval)

        # Apply range limits
        minval = max(minval, self.first)
        maxval = min(maxval, self.last) if self.last is not None else maxval
        nextval = max(minval, startval)

        # Apply the step if defined
        if self.step:
            distance_to_next = (self.step - (nextval - minval)) % self.step
            nextval += distance_to_next

        return nextval if nextval <= maxval else None

    def __str__(self) -> str:
        if self.last != self.first and self.last is not None:
            rangeval = f"{self.first}-{self.last}"
        else:
            rangeval = str(self.first)

        if self.step:
            return f"{rangeval}/{self.step}"

        return rangeval


# @attrs.define(kw_only=True, slots=True)
class MonthRangeExpression(RangeExpression):
    value_re: ClassVar[Pattern] = re.compile(
        r"(?P<first>[a-z]+)(?:-(?P<last>[a-z]+))?", re.IGNORECASE
    )

    def __init__(self, first: str, last: str | None = None):
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

        super().__init__(first=first_num, last=last_num)

    def __str__(self) -> str:
        if self.last != self.first and self.last is not None:
            return f"{MONTHS[self.first - 1]}-{MONTHS[self.last - 1]}"

        return MONTHS[self.first - 1]


@attrs.define(kw_only=True, slots=True)
class WeekdayRangeExpression(RangeExpression):
    value_re: ClassVar[Pattern] = re.compile(
        r"(?P<first>[a-z]+)(?:-(?P<last>[a-z]+))?", re.IGNORECASE
    )

    def __init__(self, first: str, last: str | None = None):
        first_num = get_weekday_index(first)
        last_num = get_weekday_index(last) if last else None
        self.__attrs_init__(first=first_num, last=last_num)

    def __str__(self) -> str:
        if self.last != self.first and self.last is not None:
            return f"{WEEKDAYS[self.first]}-{WEEKDAYS[self.last]}"

        return WEEKDAYS[self.first]


@attrs.define(kw_only=True, slots=True)
class WeekdayPositionExpression(AllExpression):
    options: ClassVar[tuple[str, ...]] = ("1st", "2nd", "3rd", "4th", "5th", "last")
    value_re: ClassVar[Pattern] = re.compile(
        r"(?P<option_name>%s) +(?P<weekday_name>(?:\d+|\w+))" % "|".join(options),
        re.IGNORECASE,
    )

    option_num: int
    weekday: int

    def __init__(self, *, option_name: str, weekday_name: str):
        option_num = self.options.index(option_name.lower())
        try:
            weekday = WEEKDAYS.index(weekday_name.lower())
        except ValueError:
            raise ValueError(f"Invalid weekday name {weekday_name!r}") from None

        self.__attrs_init__(option_num=option_num, weekday=weekday)

    def get_next_value(self, dateval: datetime, field: BaseField) -> int | None:
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

    def __str__(self) -> str:
        return f"{self.options[self.option_num]} {WEEKDAYS[self.weekday]}"


class LastDayOfMonthExpression(AllExpression):
    value_re: ClassVar[Pattern] = re.compile(r"last", re.IGNORECASE)

    def __init__(self) -> None:
        super().__init__(None)

    def get_next_value(self, dateval: datetime, field: BaseField) -> int | None:
        return monthrange(dateval.year, dateval.month)[1]

    def __str__(self) -> str:
        return "last"
