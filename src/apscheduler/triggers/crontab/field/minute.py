from __future__ import annotations

from ..rule.minute import MINUTE_RULES
from .base.base import CronBaseField


class CronMinuteField(CronBaseField):

    ALL_RULES = MINUTE_RULES

    MIN = 0
    MAX = 59

    DATE_NAME = "minute"

    DEFAULT = 0
