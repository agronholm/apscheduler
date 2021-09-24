from .all import CronMinuteAllRule
from .range import CronMinuteRangeRule


MINUTE_RULES = (
    CronMinuteAllRule,
    CronMinuteRangeRule
)
