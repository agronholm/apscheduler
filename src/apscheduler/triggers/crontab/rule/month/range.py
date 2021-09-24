from ..base.range import CronRangeRule

from ...utils.constant import MONTHS_RE, MONTHS


class CronMonthRangeRule(CronRangeRule):
    
    """
    e.g.:
        > jan-dez
        > 1-12
    """
    
    REGEX = rf'^(?P<first>{MONTHS_RE})(-(?P<last>{MONTHS_RE}))?(/(?P<step>\d+))?$'
    
    def parse(self, value, value_type):
        
        if value == 'step':
            return super(CronMonthRangeRule, self).parse(value, value_type)

        if value.isnumeric():
        
            try:
                return int(value), True
        
            except ValueError:
                return None, None
    
        else:
        
            try:
                return MONTHS.index(value.lower()) + 1, False
        
            except ValueError:
                return None, None
