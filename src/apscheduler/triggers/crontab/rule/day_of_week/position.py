from __future__ import annotations

from ...utils.calendar import get_weekday_in_month, parse_weekday, resolve_day_name
from ...utils.constant import (
    WEEKDAY_OPTIONS_LONG,
    WEEKDAY_OPTIONS_LONG_RE,
    WEEKDAY_OPTIONS_SHORT,
    WEEKDAY_OPTIONS_SHORT_RE,
    WEEKDAYS_RE,
    WEEKDAYS_SUNDAY,
)
from ...utils.utils import str_lower_strip
from ..base.range import CronRangeRule


class CronDayOfWeekPositionRule(CronRangeRule):

    OPTIONS = {"short": WEEKDAY_OPTIONS_SHORT, "long": WEEKDAY_OPTIONS_LONG}

    REGEX = {
        "short": rf"(?P<first>({WEEKDAYS_RE}))(?P<step>{WEEKDAY_OPTIONS_SHORT_RE})",
        "long": rf"(?P<step>{WEEKDAY_OPTIONS_LONG_RE})-(?P<first>({WEEKDAYS_RE}))",
    }

    def __init__(self, first, step, rule_id, min_val=0, max_val=9999):

        self.rule_id = rule_id
        self.step_options = self.OPTIONS[self.rule_id]
        self.first_is_numeric = True

        super().__init__(first=first, step=step, min_val=min_val, max_val=max_val)

    def parse(self, value, value_type):

        if value_type == "first":

            first, is_num = parse_weekday(value)
            if first is None:
                raise ValueError(f'Invalid weekday "{value}"')
            return first, is_num

        elif value_type == "step":

            try:
                return (
                    self.OPTIONS[self.rule_id].index(str_lower_strip(value, "")),
                    False,
                )

            except ValueError:
                raise ValueError(f'Invalid step value "{value}"')

    def prettify(self, value, is_numeric=None):

        if is_numeric == "step":
            return self.step_options[value]
        return resolve_day_name(is_numeric, value)

    def _validate_step(self):

        if self.step > 6:
            raise ValueError(
                f'step value {self.prettify(self.step, "step")} is invalid'
            )

    def next(self, date, field):

        weekdays = get_weekday_in_month(
            date.year, date.month, WEEKDAYS_SUNDAY[self.first]
        )

        if self.step == 5:
            weekday = weekdays[-1]

        else:
            try:
                weekday = weekdays[self.step]

            except IndexError:
                return None

        if weekday >= date.day:
            return weekday

    def __str__(self):

        step = self.prettify(self.step, "step")
        name = self.prettify(self.first, self.first_is_numeric)

        if self.rule_id == "short":
            return f"{name}{step}"
        else:
            return f"{step}-{name}"

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"step='{self.prettify(self.step, 'step')}',"
            f"first='{self.prettify(self.first, self.first_is_numeric)}')"
        )
