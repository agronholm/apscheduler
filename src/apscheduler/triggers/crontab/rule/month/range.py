from __future__ import annotations

from ...utils.constant import MONTHS, MONTHS_RE
from ..base.range import CronRangeRule


class CronMonthRangeRule(CronRangeRule):

    """
    e.g.:
        > jan-dez
        > 1-12
    """

    REGEX = rf"^(?P<first>{MONTHS_RE})(-(?P<last>{MONTHS_RE}))?(/(?P<step>\d+))?$"

    def parse(self, value, value_type):

        if value == "step":
            return super().parse(value, value_type)

        if value.isnumeric():

            try:
                return int(value), True

            except ValueError:
                return None, None

        else:

            try:
                return MONTHS.index(value.lower()) + 1, False

            except ValueError:
                return None, None
