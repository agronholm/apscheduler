from __future__ import annotations

from ...utils.constant import MONTH_RANGE_RE
from ..base.range import CronRangeRule


class CronDayOfMonthRangeRule(CronRangeRule):

    REGEX = (
        rf"^(?P<first>{MONTH_RANGE_RE})(-(?P<last>{MONTH_RANGE_RE}))?(/(?P<step>\d+))?$"
    )
