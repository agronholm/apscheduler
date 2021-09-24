from ..base.range import CronRangeRule


class CronAllRule(CronRangeRule):
    
    REGEX = r'^\*(/(?P<step>\d))?'
    
    def __init__(self, step=None, min_val=0, max_val=9999):
        
        super(CronAllRule, self).__init__(
            first=min_val,
            last=max_val,
            step=step
        )
        
    def next(self, date, field):

        min_val = max(field.min_val(date), self.first)
        
        max_val = field.max_val(date)
        if self.last is not None:
            max_val = min(max_val, self.last)
            
        next_val = max(min_val, field.cur_val(date))

        if self.step:
            next_val += (self.step - (next_val - min_val)) % self.step
    
        return next_val if next_val <= max_val else None
    
    def __eq__(self, other):
        
        return (
                isinstance(other, self.__class__)
                and
                self.step == other.step
        )
