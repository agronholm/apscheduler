from ..base.base import CronBaseRule
from ...utils.calendar import monthcalendar, monthrange


class CronDayOfMonthLastBaseRule(CronBaseRule):

    def __init__(self, **kwargs):
        
        super(CronDayOfMonthLastBaseRule, self).__init__()
    
    def next(self, date, field):
        
        return date.day
    
    def validate(self):
        
        """
        
        nothing to validate
        
        :return:
        """
        
        pass
    
    def __eq__(self, other):
        
        return self.__class__ == other.__class__
    
    def __repr__(self):
        
        return f"{self.__class__.__name__}()"

    def __str__(self):
        
        if isinstance(self.REGEX, str):
            return self.REGEX
        
        elif hasattr(self.REGEX, 'expression'):
            return self.REGEX.expression
        
        else:
            return '<unknown>'
    
    
class CronLastDayOfMonthRule(CronDayOfMonthLastBaseRule):
    
    REGEX = '^l$'
    
    def next(self, date, field):
        
        return monthrange(date.year, date.month)[1]


class CronLastBusinessDayOfMonthRule(CronDayOfMonthLastBaseRule):
    
    REGEX = '^lw$'
    
    def next(self, date, field):
    
        last = max(monthcalendar(date.year, date.month)[-1][:5])
        if last >= date.day:
            return last
