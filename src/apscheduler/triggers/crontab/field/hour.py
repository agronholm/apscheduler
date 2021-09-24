from .base.base import CronBaseField

from ..rule.hour import HOUR_RULES


class CronHourField(CronBaseField):
    
    ALL_RULES = HOUR_RULES
    
    MIN = 0
    MAX = 23

    DATE_NAME = 'hour'
    
    DEFAULT = 0
