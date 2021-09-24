from .day_of_month import CronDayOfMonthField
from .day_of_week import CronDayOfWeekField
from .hour import CronHourField
from .minute import CronMinuteField
from .month import CronMonthField
from .second import CronSecondField
from .year import CronYearField


EXPRESSION_ORDER = (
    'second',
    'minute',
    'hour',
    'day_of_month',
    'month',
    'day_of_week',
    'year'
)

CALCULATION_ORDER = (
    'year',
    'month',
    'day_of_month',
    'day_of_week',
    'hour',
    'minute',
    'second'
)

CLASS_MAP = {
    'second': CronSecondField,
    'minute': CronMinuteField,
    'hour': CronHourField,
    'day_of_month': CronDayOfMonthField,
    'month': CronMonthField,
    'day_of_week': CronDayOfWeekField,
    'year': CronYearField
}

FIELD_DEFAULTS = {
    'second': 0,
    'minute': 0,
    'hour': 0,
    'day_of_month': '?',
    'month': 1,
    'day_of_week': '*',
    # 'week': '*',
    'year': '*'
}


def set_index():
    
    global CLASS_MAP
    
    for i, name in enumerate(EXPRESSION_ORDER):
        
        kls = CLASS_MAP[name]
        
        kls.CRONTAB_INDEX = i+1
        kls.CALCULATION_INDEX = CALCULATION_ORDER.index(name)
        kls.REAL_NAME = name
        kls.PRETTY_NAME = name.replace("_", " ").title()
        

      
set_index()
