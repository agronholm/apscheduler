from __future__ import annotations

from ...utils.calendar import monthrange
from ...utils.constant import MONTH_RANGE_RE
from ..base.base import CronBaseRule


class CronDaysBeforeEndOfMonthRule(CronBaseRule):

    REGEX = rf"l-(?P<step>{MONTH_RANGE_RE})"

    def __init__(self, step, min_val=0, max_val=9999):

        super().__init__()

        self.min_val = min_val
        self.max_val = max_val

        try:
            self.step = int(step)

        except TypeError:
            raise ValueError(f"step value {step} is not numeric")

    def validate(self):

        if self.step > self.max_val:
            raise ValueError(
                "step value for n days before the end of the month must be greater than 31"
            )

    def next(self, date, field):

        calculated_day = monthrange(date.year, date.month)[1] - self.step

        if 0 < calculated_day >= date.day:
            return calculated_day

    def __str__(self):

        return f"l-{self.step}"

    def __repr__(self):

        return f"{self.__class__.__name__}(step_back='{self.step}')"
