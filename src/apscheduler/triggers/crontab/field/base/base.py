from ...rule.base.all import CronAllRule
from ...rule.base.question_mark import CronQuestionMarkRule
from ...rule.day_of_week.position import CronDayOfWeekPositionRule

from ...utils.exception import CronParserError
from ...utils.regex import MultiRe, re_compile_ignore_case
from ...utils.utils import str_lower_strip


class CronBaseField(object):
    
    __slots__ = 'crontab', 'pattern', 'rules', 'is_default'
    
    PICKLE_VERSION = 1
    
    REAL = True
    MIN = 0
    MAX = 0
    
    DATE_NAME = ''
    REAL_NAME = ''
    PRETTY_NAME = ''
    
    SEPARATOR = ','
    ALL_RULES = ()
    CRONTAB_INDEX = -1
    CALCULATION_INDEX = -1
    
    needs_weekday_position = False
    
    def __init__(self, pattern, crontab, is_default=False):
        
        super(CronBaseField, self).__init__()

        self.is_default = is_default
        self.crontab = crontab
        self.pattern = ''
        self.rules = []
        
        self.parse(pattern)

    def min_val(self, date):
        
        return self.MIN

    def max_val(self, date):
        
        return self.MAX

    def cur_val(self, date):
        
        return getattr(date, self.DATE_NAME, None)

    def next(self, date):
        
        smallest = None
        for rule in self.rules:
            value = rule.next(date, self)
            if smallest is None or (value is not None and value < smallest):
                smallest = value
    
        return smallest

    def append(self, expression):
    
        self.rules.append(expression)

    def has_rule(self):
    
        return len(self.rules) > 0

    def is_empty(self):
        
        return not self.has_rule()
    
    def is_question_mark(self):
    
        for rule in self.rules:
            if not isinstance(rule, CronQuestionMarkRule):
                return False
    
        return True

    def pop_question_mark(self):
    
        found = None
    
        for i, rule in enumerate(self.rules):
            if isinstance(rule, CronQuestionMarkRule):
                found = i
    
        if found is not None:
            return self.rules.pop(found)
        
    def add_question_mark(self):
    
        self.append(CronQuestionMarkRule())
        
    def is_all(self):
    
        if len(self.rules) == 1 and isinstance(self.rules[0], CronAllRule) and not self.rules[0].step:
            return True
        return False

    def skip_field(self):
    
        if self.is_question_mark() or self.is_all():
            return True
        return False

    def has_weekday_position(self):
    
        for rule in self.rules:
            if isinstance(rule, CronDayOfWeekPositionRule):
                return True
        return False

    def pop_weekday_position(self):
    
        found = None
    
        for i, rule in enumerate(self.rules):
            if isinstance(rule, CronDayOfWeekPositionRule):
                found = i
    
        if found is not None:
            return self.rules.pop(found)

    def parse(self, pattern):
    
        pattern = self.pattern = str_lower_strip(pattern)
        self.rules = []
    
        if not self.ALL_RULES:
            return
    
        # Split a comma-separated expression list, if any
        for expr in pattern.split(self.SEPARATOR):
        
            found, exceptions = self._parse(expr, 'REGEX')
        
            if not found and not exceptions:
            
                found, exceptions = self._parse(expr, 'FALLBACK')
        
            if not found:
            
                raise CronParserError(
                    expr,
                    self.REAL_NAME,
                    self.CRONTAB_INDEX,
                    self.crontab,
                    exceptions
                )

    def validate(self):
    
        if len(self.rules) == 0:
        
            raise CronParserError(
                self.pattern,
                self.REAL_NAME,
                self.CRONTAB_INDEX,
                self.crontab,
                [f'no matching rules found']
            )

    def _parse(self, expr, re_name):
    
        found = False
        exceptions = []
    
        for rule in self.ALL_RULES:
        
            match = self._check(rule, re_name, expr)
        
            if isinstance(match, rule):
                self.append(match)
                found = True
                break
        
            elif isinstance(match, Exception):
                exceptions.append(str(match))
    
        return found, exceptions

    def _check(self, rule, re_name, expression):
    
        if hasattr(rule, re_name):
        
            regex = getattr(rule, re_name)
        
            if isinstance(regex, str):
                regex = re_compile_ignore_case(regex)
                setattr(rule, re_name, regex)
        
            if isinstance(regex, dict):
                regex = MultiRe(**regex)
                setattr(rule, re_name, regex)
        
            match = regex.match(expression)
        
            if match:
            
                try:
                    return self._init_rule(rule, match)
            
                except ValueError as e:
                    return e

    def _init_rule(self, rule, match, min_val=None, max_val=None):
    
        rule = rule(
            **match.groupdict(),
            min_val=min_val or self.MIN,
            max_val=max_val or self.MAX
        )
    
        rule.validate()
    
        return rule
    
    def __str__(self):
        
        return self.pattern
    
    def __repr__(self):

        return f'{self.__class__.__name__}({", ".join(repr(rule) for rule in self.rules)})'
    
    def __getstate__(self):
        
        return {
            'version': self.PICKLE_VERSION,
            'is_default': self.is_default,
            'pattern': self.pattern,
            'crontab': self.crontab,
            'rules': self.rules,
        }
    
    def __setstate__(self, state):
    
        version = state.pop('version', 1)
    
        if version > self.PICKLE_VERSION:
            raise ValueError(
                f'Got serialized data for version {version} of {self.__class__.__name__}, '
                f'but only versions up to {self.PICKLE_VERSION} can be handled'
            )
    
        for key, val in state.items():
            setattr(self, key, val)
        
