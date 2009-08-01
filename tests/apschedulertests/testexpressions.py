from datetime import datetime

from nose.tools import eq_, raises

from apscheduler.expressions import *


def test_all_expression():
    expr = AllExpression()
    eq_(str(expr), '*')
    date = datetime(2009, 7, 1)
    eq_(expr.get_next_value(date, 'day'), 1)
    date = datetime(2009, 7, 10)
    eq_(expr.get_next_value(date, 'day'), 10)
    date = datetime(2009, 7, 30)
    eq_(expr.get_next_value(date, 'day'), 30)

def test_all_expression_step():
    expr = AllExpression(3)
    eq_(str(expr), '*/3')
    date = datetime(2009, 7, 1, 0)
    eq_(expr.get_next_value(date, 'hour'), 0)
    date = datetime(2009, 7, 1, 2)
    eq_(expr.get_next_value(date, 'hour'), 3)
    date = datetime(2009, 7, 1, 7)
    eq_(expr.get_next_value(date, 'hour'), 9)

@raises(ValueError)
def test_all_expression_invalid():
    AllExpression(0)

def test_range_expression():
    expr = RangeExpression(2, 9)
    eq_(str(expr), '2-9')
    date = datetime(2009, 7, 1)
    eq_(expr.get_next_value(date, 'day'), 2)
    date = datetime(2009, 7, 10)
    eq_(expr.get_next_value(date, 'day'), None)
    date = datetime(2009, 7, 5)
    eq_(expr.get_next_value(date, 'day'), 5)

def test_range_expression_step():
    expr = RangeExpression(2, 9, 3)
    eq_(str(expr), '2-9/3')
    date = datetime(2009, 7, 1)
    eq_(expr.get_next_value(date, 'day'), 2)
    date = datetime(2009, 7, 3)
    eq_(expr.get_next_value(date, 'day'), 5)
    date = datetime(2009, 7, 9)
    eq_(expr.get_next_value(date, 'day'), None)

def test_range_expression_single():
    expr = RangeExpression(9)
    eq_(str(expr), '9')
    date = datetime(2009, 7, 1)
    eq_(expr.get_next_value(date, 'day'), 9)
    date = datetime(2009, 7, 9)
    eq_(expr.get_next_value(date, 'day'), 9)
    date = datetime(2009, 7, 10)
    eq_(expr.get_next_value(date, 'day'), None)

@raises(ValueError)
def test_range_expression_invalid():
    RangeExpression(5, 3)

def test_weekday_single():
    expr = WeekdayRangeExpression('WED')
    eq_(str(expr), 'wed')
    date = datetime(2008, 2, 4)
    eq_(expr.get_next_value(date, 'day_of_week'), 2)

def test_weekday_range():
    expr = WeekdayRangeExpression('TUE', 'SAT')
    eq_(str(expr), 'tue-sat')
    date = datetime(2008, 2, 7)
    eq_(expr.get_next_value(date, 'day_of_week'), 3)

def test_weekday_pos_1():
    expr = WeekdayPositionExpression('1st', 'Fri')
    eq_(str(expr), '1st fri')
    date = datetime(2008, 2, 1)
    eq_(expr.get_next_value(date, 'day_of_week'), 1)
    
def test_weekday_pos_2():
    expr = WeekdayPositionExpression('2nd', 'wed')
    eq_(str(expr), '2nd wed')
    date = datetime(2008, 2, 1)
    eq_(expr.get_next_value(date, 'day_of_week'), 13)

def test_weekday_pos_3():
    expr = WeekdayPositionExpression('last', 'fri')
    eq_(str(expr), 'last fri')
    date = datetime(2008, 2, 1)
    eq_(expr.get_next_value(date, 'day_of_week'), 29)

@raises(ValueError)
def test_day_of_week_invalid_pos():
    WeekdayPositionExpression('6th', 'fri')

@raises(ValueError)
def test_day_of_week_invalid_name():
    WeekdayPositionExpression('1st', 'moh')

@raises(ValueError)
def test_day_of_week_invalid_first():
    WeekdayRangeExpression('moh', 'fri')

@raises(ValueError)
def test_day_of_week_invalid_last():
    WeekdayRangeExpression('mon', 'fre')
