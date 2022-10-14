from __future__ import annotations

from .all import CronMinuteAllRule
from .range import CronMinuteRangeRule

MINUTE_RULES = (CronMinuteAllRule, CronMinuteRangeRule)
