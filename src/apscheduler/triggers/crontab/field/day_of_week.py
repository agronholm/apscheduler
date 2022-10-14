from __future__ import annotations

from ..rule.day_of_week import DAY_OF_WEEK_RULES, CronDayOfWeekPositionRule
from .base.day import CronDayBaseField


class CronDayOfWeekField(CronDayBaseField):

    REAL = False

    ALL_RULES = DAY_OF_WEEK_RULES

    needs_week_day_position = False

    MIN = 0
    MAX = 6

    DATE_NAME = "day"

    DEFAULT = "*"

    def cur_val(self, date_value):

        weekday = date_value.weekday() + 1
        if weekday > 6:
            return 0
        return weekday

    def _init_rule(self, rule, match, min_val=None, max_val=None):

        if isinstance(rule, CronDayOfWeekPositionRule):

            min_val = 1
            max_val = 31

        return super()._init_rule(
            rule=rule, match=match, min_val=min_val, max_val=max_val
        )
