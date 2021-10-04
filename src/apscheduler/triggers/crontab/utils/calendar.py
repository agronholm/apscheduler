from calendar import monthcalendar, monthrange as _monthrange
from sys import getsizeof as sys_getsizeof

from .constant import WEEKDAYS_MONDAY, WEEKDAYS_SUNDAY, BUSINESS_DAYS


MONTHRANGE_CACHE = {}
WEEKDAY_CACHE = {}
BUSINESS_DAYS_CACHE = {}

MAX_CACHE_SIZE = 2097152  # 2 MB


def monthrange(year, month):
    
    global MONTHRANGE_CACHE
    
    if sys_getsizeof(MONTHRANGE_CACHE) > MAX_CACHE_SIZE:
        MONTHRANGE_CACHE = {}
    
    args = (year, month)
    
    if not MONTHRANGE_CACHE or args not in MONTHRANGE_CACHE:
        MONTHRANGE_CACHE[args] = _monthrange(*args)
    
    return MONTHRANGE_CACHE[args]


def get_weekday_in_month(year, month, day):
    
    global WEEKDAY_CACHE
    
    if sys_getsizeof(WEEKDAY_CACHE) > MAX_CACHE_SIZE:
        WEEKDAY_CACHE = {}
    
    if not WEEKDAY_CACHE or (year, month, day) not in WEEKDAY_CACHE:
        
        days = []
        
        for week_list in monthcalendar(year, month):
            days.append(dict(zip(WEEKDAYS_MONDAY, week_list)).pop(day))
        
        WEEKDAY_CACHE[(year, month, day)] = tuple([day for day in days if day > 0])
    
    return WEEKDAY_CACHE[(year, month, day)]


def order_weekdays(week_starts_at='sun'):
    
    if week_starts_at not in WEEKDAYS_SUNDAY:
        week_starts_at = 'sun'
    
    if WEEKDAYS_SUNDAY[0] == week_starts_at:
        return WEEKDAYS_SUNDAY
    
    if WEEKDAYS_MONDAY[0] == week_starts_at:
        return WEEKDAYS_MONDAY
    
    weekdays = list(WEEKDAYS_SUNDAY)
    while weekdays[0] != week_starts_at:
        weekdays.append(weekdays.pop(0))
    
    return weekdays


def parse_weekday(weekday):

    is_numeric = False
    
    if weekday.isnumeric():
        
        is_numeric = True
        
        try:
            weekday = int(weekday)
        
        except ValueError:
            weekday = None
        
        else:
            if 1 > weekday > 7:
                weekday = None
        
        if weekday:
            weekday = weekday - 1
    
    else:
        
        try:
            weekday = WEEKDAYS_SUNDAY.index(weekday.lower())
        
        except ValueError:
            weekday = None
    
    return weekday, is_numeric


def resolve_day_name(is_numeric, position):
    
    if is_numeric:
        return position + 1
    
    try:
        return WEEKDAYS_SUNDAY[position]
    except IndexError:
        return 'nan'


def get_business_days(year, month):
    
    global BUSINESS_DAYS_CACHE
    
    if sys_getsizeof(BUSINESS_DAYS_CACHE) > MAX_CACHE_SIZE:
        BUSINESS_DAYS_CACHE = {}
        
    args = (year, month)
    
    if not BUSINESS_DAYS_CACHE or args not in BUSINESS_DAYS_CACHE:
        
        business_days = {}
        
        for week_list in monthcalendar(*args):
            business_days.update(
                zip(week_list, BUSINESS_DAYS)
            )
        
        if 0 in business_days:
            del business_days[0]
        
        BUSINESS_DAYS_CACHE[args] = business_days
    
    return BUSINESS_DAYS_CACHE[args]
