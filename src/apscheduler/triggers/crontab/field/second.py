from .base.base import CronBaseField

from ..rule.second import SECOND_RULES


class CronSecondField(CronBaseField):
    
    ALL_RULES = SECOND_RULES
    
    MIN = 0
    MAX = 59
    
    DATE_NAME = 'second'

    DEFAULT = 0