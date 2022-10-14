from __future__ import annotations

from .all import CronHourAllRule
from .range import CronHourRangeRule

HOUR_RULES = (CronHourAllRule, CronHourRangeRule)
