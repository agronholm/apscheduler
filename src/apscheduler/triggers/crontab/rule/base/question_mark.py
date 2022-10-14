from __future__ import annotations

from ..base.all import CronAllRule


class CronQuestionMarkRule(CronAllRule):

    REGEX = r"\?"

    def __init__(self, min_val=0, max_val=9999):
        super().__init__(min_val=min_val, max_val=max_val)

    def next(self, date, field):

        return field.cur_val(date)

    def __eq__(self, other):

        return isinstance(other, self.__class__)

    def __str__(self):

        return "?"

    def __repr__(self):

        return f"{self.__class__.__name__}()"
