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

def test_day_of_week_1():
    expr = DayOfWeekExpression('1st', 'Fri')
    eq_(str(expr), '1st fri')
    date = datetime(2008, 2, 1)
    eq_(expr.get_next_value(date, 'day_of_week'), 1)
    
def test_day_of_week_2():
    expr = DayOfWeekExpression('2nd', 'wed')
    eq_(str(expr), '2nd wed')
    date = datetime(2008, 2, 1)
    eq_(expr.get_next_value(date, 'day_of_week'), 13)

def test_day_of_week_3():
    expr = DayOfWeekExpression('last', 'fri')
    eq_(str(expr), 'last fri')
    date = datetime(2008, 2, 1)
    eq_(expr.get_next_value(date, 'day_of_week'), 29)

@raises(ValueError)
def test_day_of_week_invalid_pos():
    DayOfWeekExpression('6th', 'fri')

@raises(ValueError)
def test_day_of_week_invalid_name():
    DayOfWeekExpression('1st', 'moh')
