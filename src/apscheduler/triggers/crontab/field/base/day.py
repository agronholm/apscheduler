from .base import CronBaseField
from ...rule.base.question_mark import CronQuestionMarkRule


class CronDayBaseField(CronBaseField):
    
    def parse(self, expr):
        
        super(CronDayBaseField, self).parse(expr)
        
        if len(self.rules) > 1:
            
            rules_to_pop = []
            
            for i, rule in enumerate(self.rules):
                if isinstance(rule, CronQuestionMarkRule):
                    rules_to_pop.append(i)
            
            for i in rules_to_pop:
                self.rules.pop(i)
