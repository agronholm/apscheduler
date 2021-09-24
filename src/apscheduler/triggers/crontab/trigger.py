from tzlocal import get_localzone
from datetime import datetime

from .field import (
    EXPRESSION_ORDER,
    CALCULATION_ORDER,
    CLASS_MAP

)
from .rule.at import AT_RULES
from .utils.exception import CronParserError
from .utils.utils import str_lower_strip, ceil_date, fix_daylight_saving_time_shift
from .utils.constant import MICROSECOND_TIMEDELTA

from ...marshalling import unmarshal_timezone, unmarshal_date, marshal_date, marshal_timezone
from ...validators import as_timezone, as_date


class CronTabTrigger(object):

    __slots__ = (
        'expression', 'expression_map', 'fields',
        'timezone', 'start_date', 'end_date', 'last_date'
    )

    PICKLE_VERSION = 1
    
    def __init__(
            self,
            expression,
            last_date=None,
            start_date=None,
            end_date=None,
            timezone=None
    ):
        
        super(CronTabTrigger, self).__init__()
        
        self.expression = None
        self.expression_map = self._split_expression(expression)
        
        self.timezone = as_timezone(timezone)
        self.start_date = as_date(start_date) or datetime.now()
        self.end_date = as_date(end_date)
        self.last_date = as_date(last_date)

        self.fields = []

        self._init_fields()
        self._validate()
    
    def next(self, previous_fire_time=None, now=None):
    
        next_date = self._prepare_next(previous_fire_time, now)
    
        field_no = 0

        while 0 <= field_no < len(self.fields):
        
            field = self.fields[field_no]
        
            if field.skip_field():
                field_no += 1
                continue
        
            curr_value = field.cur_val(next_date)
            next_value = field.next(next_date)
        
            if next_value is None:
                # No valid value was found
                next_date, field_no = self._increment_date(next_date, field_no - 1)
        
            elif next_value > curr_value:
            
                # A valid, but higher than the starting value, was found
                if field.REAL:
                    next_date = self._next_date(next_date, field_no, next_value)
                    field_no += 1
            
                else:
                    next_date, field_no = self._increment_date(next_date, field_no)
        
            else:
                # A valid value was found, no changes necessary
                field_no += 1
        
            # Return if the date has rolled past the end date
            if self.end_date and next_date > self.end_date:
                return None

        return self._finish_next(next_date, field_no)

    get_next_fire_time = next

    def _prepare_next(self, last_date=None, now=None):
    
        if last_date:
            start_date = min(now, last_date + MICROSECOND_TIMEDELTA)
            if start_date == last_date:
                start_date += MICROSECOND_TIMEDELTA
        else:
            start_date = (self.last_date if self.last_date else self.start_date) + MICROSECOND_TIMEDELTA

        start_date = ceil_date(start_date)
    
        return fix_daylight_saving_time_shift(
            start_date, start_date.astimezone(self.timezone)
        )
    
    def _finish_next(self, next_date, field_no):
        
        if field_no >= 0:
        
            if self.end_date and next_date > self.end_date:
                return None
            self.last_date = next_date
            return next_date
        
    def _diffed_date(self, last_date, next_date):
    
        """
        daylight saving time problem fix
        some values are not calculated because the datetime object
        gets messed up when the daylight saving time border is crossed
        """
    
        return fix_daylight_saving_time_shift(
            last_date,
            last_date + (next_date - last_date.replace(tzinfo=None))
        )

    def _increment_date(self, date, field_no):
    
        values = {'tzinfo': self.timezone}
        i = 0
    
        while i < len(self.fields):
        
            field = self.fields[i]
            date_name = field.DATE_NAME
        
            if not field.REAL:
            
                if i == field_no:
                    field_no -= 1
                    i -= 1
            
                else:
                    i += 1
        
            elif i < field_no:
            
                values[date_name] = field.cur_val(date)
                i += 1
        
            elif i > field_no:
            
                values[date_name] = field.min_val(date)
                i += 1
        
            else:
                value = field.cur_val(date)
                max_val = field.max_val(date)
            
                if value == max_val:
                    field_no -= 1
                    i -= 1
            
                else:
                    values[field.DATE_NAME] = value + 1
                    i += 1

        return fix_daylight_saving_time_shift(
            date,
            datetime(**values)
        ), field_no

    def _next_date(self, date, field_no, new_value):
    
        values = {'tzinfo': self.timezone}

        for index, field in enumerate(self.fields):
        
            date_name = field.DATE_NAME
        
            if field.REAL:
            
                if index < field_no:
                    values[date_name] = field.cur_val(date)
            
                elif index > field_no:
                    values[date_name] = field.min_val(date)
            
                else:
                    values[date_name] = new_value
    
        return datetime(**values)
    
    def _init_fields(self):
        
        weekday_position_rule = None
        
        for field_name in CALCULATION_ORDER:
        
            kls = CLASS_MAP[field_name]
        
            field = kls(
                pattern=self.expression_map.get(field_name, kls.DEFAULT),
                crontab=self.expression,
                is_default=field_name not in self.expression_map
            )
        
            if field.has_weekday_position() and not field.needs_weekday_position:
                
                weekday_position_rule = field.pop_weekday_position()
                if field.is_empty():
                    field.add_question_mark()
                    
            self.fields.append(field)

        self._move_weekday_position(weekday_position_rule)
        
    def _move_weekday_position(self, weekday_position_rule):
        
        if weekday_position_rule:
            for field in self.fields:
                if field.needs_weekday_position:
                    if field.is_question_mark():
                        field.pop_question_mark()
                    field.append(weekday_position_rule)
        
    def _validate(self):
        
        for field in self.fields:
            field.validate()
            
    @staticmethod
    def _check_5(expression):
        
        first = expression.lstrip(' ', 1)[0]
        last = expression.rstrip(' ', 1)[-1]
    
        if first == last and (
                first == last == '1/1'
                or
                first == last == '*/1'
        ):
            # doesnt matter
            field_names = EXPRESSION_ORDER[:-1]
    
        elif first.isnumeric() and -1 < int(first) < 61:
            # seconds
            field_names = EXPRESSION_ORDER[:-1]
    
        elif last.isnumeric() and 1899 < int(last):
            # years
            field_names = EXPRESSION_ORDER[1:]
    
        else:
            raise CronParserError(
                None, None, None, expression,
                f'Could not determine if the first field is second or the last field is year'
            )
        
        return field_names
    
    def _split_expression(self, expression):
    
        expression = self.expression = str_lower_strip(expression)
        
        if expression.startswith('@'):
            
            _expression = AT_RULES.get(expression, None)
            
            if not _expression:
                raise CronParserError(
                    None, None, None, expression,
                    f'Unknown @ expression {expression}'
                )
            
            expression = _expression
            
        spaces = expression.count(' ')
        
        if spaces == 4:
            # neither second nor year
            field_names = EXPRESSION_ORDER[1:-1]
            
        elif spaces == 5:
            # second or year
            field_names = self._check_5(expression)

        elif spaces == 6:
            field_names = EXPRESSION_ORDER
        
        else:
            raise CronParserError(
                None, None, None, expression,
                f'Wrong number of fields; got {spaces+1}, expected 5, 6 or 7 fields'
            )

        expression_map = dict(zip(field_names, expression.split()))

        return expression_map
    
    def __getstate__(self):

        return {
            'version': self.PICKLE_VERSION,
            'expression': self.expression,
            'expression_map': self.expression_map,
            'timezone': marshal_timezone(self.timezone),
            'start_date': marshal_date(self.start_date),
            'end_date': marshal_date(self.end_date),
            'last_date':  marshal_date(self.last_date),
            'fields': self.fields
        }

    def __setstate__(self, state):
    
        version = state.pop('version', 1)
    
        if version > self.PICKLE_VERSION:
            raise ValueError(
                f'Got serialized data for version {version} of {self.__class__.__name__}, '
                f'but only versions up to {self.PICKLE_VERSION} can be handled'
            )
    
        for key, val in state.items():
            if key == 'timezone':
                val = unmarshal_timezone(val) if val else get_localzone()
            elif key.endswith('date'):
                val = unmarshal_date(val)
            setattr(self, key, val)

    def __str__(self):
    
        fields = ", ".join([f"{f.REAL_NAME}='{f}'" for f in self.fields if not f.is_default])
    
        return f'crontab[{fields}]'

    def __repr__(self):
    
        fields = [f"{f.REAL_NAME}='{f}'" for f in self.fields if not f.is_default]
    
        for key in (
                'start_date',
                'end_date',
                'last_date',
                'timezone'
        ):
            val = getattr(self, key)
            if val and key.endswith('date'):
                val = val.isoformat()
            fields.append(f"key='{val}'")
    
        fields = ', '.join(fields)
    
        return f"<{self.__class__.__name__}({fields})>"
