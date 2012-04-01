from datetime import datetime

from nose.tools import eq_, raises

from apscheduler.triggers.cron.expressions import *
from apscheduler.triggers.cron.fields import *


def test_all_expression():
    field = DayOfMonthField('day', '*')
    eq_(repr(field), "DayOfMonthField('day', '*')")
    date = datetime(2009, 7, 1)
    eq_(field.get_next_value(date), 1)
    date = datetime(2009, 7, 10)
    eq_(field.get_next_value(date), 10)
    date = datetime(2009, 7, 30)
    eq_(field.get_next_value(date), 30)


def test_all_expression_step():
    field = BaseField('hour', '*/3')
    eq_(repr(field), "BaseField('hour', '*/3')")
    date = datetime(2009, 7, 1, 0)
    eq_(field.get_next_value(date), 0)
    date = datetime(2009, 7, 1, 2)
    eq_(field.get_next_value(date), 3)
    date = datetime(2009, 7, 1, 7)
    eq_(field.get_next_value(date), 9)


@raises(ValueError)
def test_all_expression_invalid():
    BaseField('hour', '*/0')


def test_all_expression_repr():
    expr = AllExpression()
    eq_(repr(expr), 'AllExpression(None)')


def test_all_expression_step_repr():
    expr = AllExpression(2)
    eq_(repr(expr), "AllExpression(2)")


def test_range_expression():
    field = DayOfMonthField('day', '2-9')
    eq_(repr(field), "DayOfMonthField('day', '2-9')")
    date = datetime(2009, 7, 1)
    eq_(field.get_next_value(date), 2)
    date = datetime(2009, 7, 10)
    eq_(field.get_next_value(date), None)
    date = datetime(2009, 7, 5)
    eq_(field.get_next_value(date), 5)


def test_range_expression_step():
    field = DayOfMonthField('day', '2-9/3')
    eq_(repr(field), "DayOfMonthField('day', '2-9/3')")
    date = datetime(2009, 7, 1)
    eq_(field.get_next_value(date), 2)
    date = datetime(2009, 7, 3)
    eq_(field.get_next_value(date), 5)
    date = datetime(2009, 7, 9)
    eq_(field.get_next_value(date), None)


def test_range_expression_single():
    field = DayOfMonthField('day', 9)
    eq_(repr(field), "DayOfMonthField('day', '9')")
    date = datetime(2009, 7, 1)
    eq_(field.get_next_value(date), 9)
    date = datetime(2009, 7, 9)
    eq_(field.get_next_value(date), 9)
    date = datetime(2009, 7, 10)
    eq_(field.get_next_value(date), None)


@raises(ValueError)
def test_range_expression_invalid():
    DayOfMonthField('day', '5-3')


def test_range_expression_repr():
    expr = RangeExpression(3, 7)
    eq_(repr(expr), 'RangeExpression(3, 7)')


def test_range_expression_single_repr():
    expr = RangeExpression(4)
    eq_(repr(expr), 'RangeExpression(4)')


def test_range_expression_step_repr():
    expr = RangeExpression(3, 7, 2)
    eq_(repr(expr), 'RangeExpression(3, 7, 2)')


def test_weekday_single():
    field = DayOfWeekField('day_of_week', 'WED')
    eq_(repr(field), "DayOfWeekField('day_of_week', 'wed')")
    date = datetime(2008, 2, 4)
    eq_(field.get_next_value(date), 2)


def test_weekday_range():
    field = DayOfWeekField('day_of_week', 'TUE-SAT')
    eq_(repr(field), "DayOfWeekField('day_of_week', 'tue-sat')")
    date = datetime(2008, 2, 7)
    eq_(field.get_next_value(date), 3)


def test_weekday_pos_1():
    expr = WeekdayPositionExpression('1st', 'Fri')
    eq_(str(expr), '1st fri')
    date = datetime(2008, 2, 1)
    eq_(expr.get_next_value(date, 'day'), 1)


def test_weekday_pos_2():
    expr = WeekdayPositionExpression('2nd', 'wed')
    eq_(str(expr), '2nd wed')
    date = datetime(2008, 2, 1)
    eq_(expr.get_next_value(date, 'day'), 13)


def test_weekday_pos_3():
    expr = WeekdayPositionExpression('last', 'fri')
    eq_(str(expr), 'last fri')
    date = datetime(2008, 2, 1)
    eq_(expr.get_next_value(date, 'day'), 29)


@raises(ValueError)
def test_day_of_week_invalid_pos():
    WeekdayPositionExpression('6th', 'fri')


@raises(ValueError)
def test_day_of_week_invalid_name():
    WeekdayPositionExpression('1st', 'moh')


def test_weekday_position_expression_repr():
    expr = WeekdayPositionExpression('2nd', 'FRI')
    eq_(repr(expr), "WeekdayPositionExpression('2nd', 'fri')")


@raises(ValueError)
def test_day_of_week_invalid_first():
    WeekdayRangeExpression('moh', 'fri')


@raises(ValueError)
def test_day_of_week_invalid_last():
    WeekdayRangeExpression('mon', 'fre')


def test_weekday_range_expression_repr():
    expr = WeekdayRangeExpression('tue', 'SUN')
    eq_(repr(expr), "WeekdayRangeExpression('tue', 'sun')")


def test_weekday_range_expression_single_repr():
    expr = WeekdayRangeExpression('thu')
    eq_(repr(expr), "WeekdayRangeExpression('thu')")


def test_last_day_of_month_expression():
    expr = LastDayOfMonthExpression()
    date = datetime(2012, 2, 1)
    eq_(expr.get_next_value(date, 'day'), 29)


def test_last_day_of_month_expression_invalid():
    expr = LastDayOfMonthExpression()
    eq_(repr(expr), "LastDayOfMonthExpression()")
