from ..base.range import CronRangeRule

from ...utils.constant import MONTH_RANGE_RE


class CronDayOfMonthRangeRule(CronRangeRule):
    
    REGEX = rf'^(?P<first>{MONTH_RANGE_RE})(-(?P<last>{MONTH_RANGE_RE}))?(/(?P<step>\d+))?$'
    