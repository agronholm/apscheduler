from __future__ import annotations

from .all import CronYearAllRule
from .range import CronYearRangeRule

YEAR_RULES = (CronYearAllRule, CronYearRangeRule)
