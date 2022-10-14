from __future__ import annotations

from ..rule.month import MONTH_RULES
from .base.base import CronBaseField


class CronMonthField(CronBaseField):

    ALL_RULES = MONTH_RULES

    MIN = 1
    MAX = 12

    DATE_NAME = "month"

    DEFAULT = 1
