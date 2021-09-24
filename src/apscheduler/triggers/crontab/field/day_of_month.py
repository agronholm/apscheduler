from .base.day import CronDayBaseField
from ..rule.day_of_month import DAY_OF_MONTH_RULES

from ..utils.calendar import monthrange


class CronDayOfMonthField(CronDayBaseField):
    
    ALL_RULES = DAY_OF_MONTH_RULES
    
    MIN = 1
    MAX = 31
    
    DATE_NAME = 'day'
    
    DEFAULT = '?'
    
    needs_weekday_position = True
    
    def max_val(self, date):
        
        return monthrange(date.year, date.month)[1]
