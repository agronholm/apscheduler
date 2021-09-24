from .base.base import CronBaseField

from ..rule.minute import MINUTE_RULES


class CronMinuteField(CronBaseField):
    
    ALL_RULES = MINUTE_RULES
    
    MIN = 0
    MAX = 59

    DATE_NAME = 'minute'
    
    DEFAULT = 0
