from .base.base import CronBaseField

from ..rule.month import MONTH_RULES


class CronMonthField(CronBaseField):
    
    ALL_RULES = MONTH_RULES
    
    MIN = 1
    MAX = 12
    
    DATE_NAME = 'month'
    
    DEFAULT = 1
