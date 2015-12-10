from datetime import datetime

import pytest

from apscheduler.triggers.cron.fields import DayOfMonthField, BaseField, DayOfWeekField
from apscheduler.triggers.cron.expressions import (
    AllExpression, RangeExpression, WeekdayPositionExpression, WeekdayRangeExpression,
    LastDayOfMonthExpression)


def test_all_expression():
    field = DayOfMonthField('day', '*')
    assert repr(field) == "DayOfMonthField('day', '*')"
    date = datetime(2009, 7, 1)
    assert field.get_next_value(date) == 1
    date = datetime(2009, 7, 10)
    assert field.get_next_value(date) == 10
    date = datetime(2009, 7, 30)
    assert field.get_next_value(date) == 30


def test_all_expression_step():
    field = BaseField('hour', '*/3')
    assert repr(field) == "BaseField('hour', '*/3')"
    date = datetime(2009, 7, 1, 0)
    assert field.get_next_value(date) == 0
    date = datetime(2009, 7, 1, 2)
    assert field.get_next_value(date) == 3
    date = datetime(2009, 7, 1, 7)
    assert field.get_next_value(date) == 9


def test_all_expression_invalid():
    pytest.raises(ValueError, BaseField, 'hour', '*/0')


def test_all_expression_repr():
    expr = AllExpression()
    assert repr(expr) == 'AllExpression(None)'


def test_all_expression_step_repr():
    expr = AllExpression(2)
    assert repr(expr) == "AllExpression(2)"


def test_range_expression():
    field = DayOfMonthField('day', '2-9')
    assert repr(field) == "DayOfMonthField('day', '2-9')"
    date = datetime(2009, 7, 1)
    assert field.get_next_value(date) == 2
    date = datetime(2009, 7, 10)
    assert field.get_next_value(date) is None
    date = datetime(2009, 7, 5)
    assert field.get_next_value(date) == 5


def test_range_expression_step():
    field = DayOfMonthField('day', '2-9/3')
    assert repr(field) == "DayOfMonthField('day', '2-9/3')"
    date = datetime(2009, 7, 1)
    assert field.get_next_value(date) == 2
    date = datetime(2009, 7, 3)
    assert field.get_next_value(date) == 5
    date = datetime(2009, 7, 9)
    assert field.get_next_value(date) is None


def test_range_expression_single():
    field = DayOfMonthField('day', 9)
    assert repr(field) == "DayOfMonthField('day', '9')"
    date = datetime(2009, 7, 1)
    assert field.get_next_value(date) == 9
    date = datetime(2009, 7, 9)
    assert field.get_next_value(date) == 9
    date = datetime(2009, 7, 10)
    assert field.get_next_value(date) is None


def test_range_expression_invalid():
    pytest.raises(ValueError, DayOfMonthField, 'day', '5-3')


def test_range_expression_repr():
    expr = RangeExpression(3, 7)
    assert repr(expr) == 'RangeExpression(3, 7)'


def test_range_expression_single_repr():
    expr = RangeExpression(4)
    assert repr(expr) == 'RangeExpression(4)'


def test_range_expression_step_repr():
    expr = RangeExpression(3, 7, 2)
    assert repr(expr) == 'RangeExpression(3, 7, 2)'


def test_weekday_single():
    field = DayOfWeekField('day_of_week', 'WED')
    assert repr(field) == "DayOfWeekField('day_of_week', 'wed')"
    date = datetime(2008, 2, 4)
    assert field.get_next_value(date) == 2


def test_weekday_range():
    field = DayOfWeekField('day_of_week', 'TUE-SAT')
    assert repr(field) == "DayOfWeekField('day_of_week', 'tue-sat')"
    date = datetime(2008, 2, 7)
    assert field.get_next_value(date) == 3


def test_weekday_pos_1():
    expr = WeekdayPositionExpression('1st', 'Fri')
    assert str(expr) == '1st fri'
    date = datetime(2008, 2, 1)
    assert expr.get_next_value(date, 'day') == 1


def test_weekday_pos_2():
    expr = WeekdayPositionExpression('2nd', 'wed')
    assert str(expr) == '2nd wed'
    date = datetime(2008, 2, 1)
    assert expr.get_next_value(date, 'day') == 13


def test_weekday_pos_3():
    expr = WeekdayPositionExpression('last', 'fri')
    assert str(expr) == 'last fri'
    date = datetime(2008, 2, 1)
    assert expr.get_next_value(date, 'day') == 29


def test_day_of_week_invalid_pos():
    pytest.raises(ValueError, WeekdayPositionExpression, '6th', 'fri')


def test_day_of_week_invalid_name():
    pytest.raises(ValueError, WeekdayPositionExpression, '1st', 'moh')


def test_weekday_position_expression_repr():
    expr = WeekdayPositionExpression('2nd', 'FRI')
    assert repr(expr) == "WeekdayPositionExpression('2nd', 'fri')"


def test_day_of_week_invalid_first():
    pytest.raises(ValueError, WeekdayRangeExpression, 'moh', 'fri')


def test_day_of_week_invalid_last():
    pytest.raises(ValueError, WeekdayRangeExpression, 'mon', 'fre')


def test_weekday_range_expression_repr():
    expr = WeekdayRangeExpression('tue', 'SUN')
    assert repr(expr) == "WeekdayRangeExpression('tue', 'sun')"


def test_weekday_range_expression_single_repr():
    expr = WeekdayRangeExpression('thu')
    assert repr(expr) == "WeekdayRangeExpression('thu')"


def test_last_day_of_month_expression():
    expr = LastDayOfMonthExpression()
    date = datetime(2012, 2, 1)
    assert expr.get_next_value(date, 'day') == 29


def test_last_day_of_month_expression_invalid():
    expr = LastDayOfMonthExpression()
    assert repr(expr) == "LastDayOfMonthExpression()"
