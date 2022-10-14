from __future__ import annotations

from ..rule.hour import HOUR_RULES
from .base.base import CronBaseField


class CronHourField(CronBaseField):

    ALL_RULES = HOUR_RULES

    MIN = 0
    MAX = 23

    DATE_NAME = "hour"

    DEFAULT = 0
