from __future__ import annotations

from .all import CronMonthAllRule
from .range import CronMonthRangeRule

MONTH_RULES = (CronMonthAllRule, CronMonthRangeRule)
