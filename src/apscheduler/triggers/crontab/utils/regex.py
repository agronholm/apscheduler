from re import compile as re_compile, IGNORECASE as RE_IGNORE_CASE


def re_compile_ignore_case(expression):
    
    return re_compile(
        expression,
        RE_IGNORE_CASE
    )


class MultiRe(object):
    
    __slots__ = 'rules',
    
    class NamedMatch(object):
        
        slots = 'match', 'rule_id'
        
        def __init__(self, match, rule_id):
            
            self.match = match
            self.rule_id = rule_id
        
        def groupdict(self):
            
            group_dict = self.match.groupdict()
            group_dict['rule_id'] = self.rule_id
            
            return group_dict
    
    def __init__(self, **rules):
        
        self.rules = {}
        self.add(**rules)
    
    def clone(self):
        
        new = self.__class__()
        new.add(**self.rules)
        
        return new
    
    @staticmethod
    def compile(pattern):

        return re_compile_ignore_case(pattern)
    
    def pop(self, key):
        
        if key in self.rules:
            self.rules.pop(key)
    
    def add(self, **rules):
        
        for rule_id, rule in rules.items():
            self.rules[rule_id] =  self.compile(rule)
    
    def match(self, pattern):
        
        for rule_id, rule in self.rules.items():
            match = rule.match(pattern)
            if match:
                return self.NamedMatch(match, rule_id)