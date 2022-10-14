from __future__ import annotations

from ..base.base import CronBaseRule


class CronRangeRule(CronBaseRule):

    REGEX = r"(?P<first>\d+)(?:-(?P<last>\d+))?(?:/(?P<step>\d+))?$"

    EXCEPTION = 'Invalid {position} value "{value}"'

    def __init__(self, first, last=None, step=None, min_val=0, max_val=9999):

        super().__init__()

        self.min_val = min_val
        self.max_val = max_val

        first_num, self.first_is_numeric = self.parse(first, "first")
        if first_num is None:
            self._raise(position="first", value=first)

        self.first = first_num

        self.last_is_numeric = None

        last_num = None
        if last is not None:
            last_num, self.last_is_numeric = self.parse(last, "last")
            if last_num is None:
                self._raise(position="last", value=last)

        self.last = last_num

        step_num = None
        if step is not None:
            step_num, _ = self.parse(step, "step")
            if step_num is None:
                self._raise(position="step", value=step)

        self.step = step_num

        self.last_from_first = False
        if self.last is None and self.step is None:
            self.last = self.first
            self.last_from_first = True
