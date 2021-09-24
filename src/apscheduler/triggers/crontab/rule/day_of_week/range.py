from ..base.range import CronRangeRule

from ...utils.calendar import parse_weekday, resolve_day_name
from ...utils.constant import WEEKDAYS_RE


class CronDayOfWeekRangeRule(CronRangeRule):
    
    """
    weekdays start at sunday so 1=sun, 2=mon etc.
    
    e.g.:
        > sun-sat
        > 1-7
        
    """
    
    REGEX = rf'^(?P<first>({WEEKDAYS_RE}))(-(?P<last>({WEEKDAYS_RE})))?(/(?P<step>[1-7]))?$'
    
    FALLBACK = r'(?P<first>\d+|\w+)(?:-(?P<last>\d+|\w+))?(?:/(?P<step>\d+))?$'
    
    EXCEPTION = 'Invalid weekday name "{value}"'
    
    def prettify(self, value, is_numeric=None):
        
        return resolve_day_name(is_numeric, value)
        
    def parse(self, value, value_type):
        
        if value_type == 'step':
            return super(CronDayOfWeekRangeRule, self).parse(value, value_type)
        return parse_weekday(value)
    
    def _validate_step(self):
        
        max_val = self.last or self.max_val + 1
        
        if self.step and self.step > max_val - self.first:
            
            raise ValueError(
                f'the step value {self.step} is higher'
                f' than the range of days '
                f'{resolve_day_name(self.first_is_numeric, self.first)}-'
                f'{resolve_day_name(self.last_is_numeric, max_val)}'
            )
