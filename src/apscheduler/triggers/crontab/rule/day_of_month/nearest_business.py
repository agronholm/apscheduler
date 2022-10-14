from __future__ import annotations

from ...utils.calendar import get_business_days
from ...utils.constant import MONTH_RANGE_RE
from .range import CronDayOfMonthRangeRule


class CronNearestBusinessDayRule(CronDayOfMonthRangeRule):

    REGEX = rf"(?P<first>{MONTH_RANGE_RE})w"

    def __init__(self, first, min_val=0, max_val=9999):

        super().__init__(first=first, last=first, min_val=min_val, max_val=max_val)

    def __eq__(self, other):

        return isinstance(other, self.__class__) and self.first == other.first

    @staticmethod
    def closest(day, day_type, last_day):

        if day_type != "bus":

            if day_type == "sat":

                if day == 1:
                    day += 2

                else:
                    day -= 1

            elif day_type == "sun":

                if day == last_day:
                    day -= 2

                else:
                    day += 1

        return day

    def next(self, date, field):

        next_day = super().next(date, field)

        if next_day:

            day_type = get_business_days(date.year, date.month)[next_day]
            next_day = self.closest(next_day, day_type, field.max_val(date))

            if next_day < date.day:
                next_day = None

        return next_day

    def __str__(self):

        return f"{self.first}w"

    def __repr__(self):

        return f"{self.__class__.__name__}(first='{self.first}')"
