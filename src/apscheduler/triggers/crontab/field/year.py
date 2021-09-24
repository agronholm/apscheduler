from .base.base import CronBaseField

from ..rule.year import YEAR_RULES


class CronYearField(CronBaseField):
    
    ALL_RULES = YEAR_RULES
    
    MIN = 1970
    MAX = 9999
    
    DATE_NAME = 'year'
    
    DEFAULT = '*'
