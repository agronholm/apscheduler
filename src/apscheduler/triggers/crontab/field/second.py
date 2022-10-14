from __future__ import annotations

from ..rule.second import SECOND_RULES
from .base.base import CronBaseField


class CronSecondField(CronBaseField):

    ALL_RULES = SECOND_RULES

    MIN = 0
    MAX = 59

    DATE_NAME = "second"

    DEFAULT = 0
