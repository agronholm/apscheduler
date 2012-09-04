from datetime import date, datetime

from apscheduler.triggers.cron.fields import *
from apscheduler.util import datetime_ceil, convert_to_datetime, iteritems


class CronTrigger(object):
    FIELD_NAMES = ('year', 'month', 'day', 'week', 'day_of_week', 'hour',
                   'minute', 'second')
    FIELDS_MAP = {'year': BaseField,
                  'month': BaseField,
                  'week': WeekField,
                  'day': DayOfMonthField,
                  'day_of_week': DayOfWeekField,
                  'hour': BaseField,
                  'minute': BaseField,
                  'second': BaseField}

    def __init__(self, **values):
        self.start_date = values.pop('start_date', None)
        if self.start_date:
            self.start_date = convert_to_datetime(self.start_date)

        # Check field names and yank out all None valued fields
        for key, value in list(iteritems(values)):
            if key not in self.FIELD_NAMES:
                raise TypeError('Invalid field name: %s' % key)
            if value is None:
                del values[key]

        self.fields = []
        assign_defaults = False
        for field_name in self.FIELD_NAMES:
            if field_name in values:
                exprs = values.pop(field_name)
                is_default = False
                assign_defaults = not values
            elif assign_defaults:
                exprs = DEFAULT_VALUES[field_name]
                is_default = True
            else:
                exprs = '*'
                is_default = True

            field_class = self.FIELDS_MAP[field_name]
            field = field_class(field_name, exprs, is_default)
            self.fields.append(field)

    def _increment_field_value(self, dateval, fieldnum):
        """
        Increments the designated field and resets all less significant fields
        to their minimum values.

        :type dateval: datetime
        :type fieldnum: int
        :type amount: int
        :rtype: tuple
        :return: a tuple containing the new date, and the number of the field
                 that was actually incremented
        """
        i = 0
        values = {}
        while i < len(self.fields):
            field = self.fields[i]
            if not field.REAL:
                if i == fieldnum:
                    fieldnum -= 1
                    i -= 1
                else:
                    i += 1
                continue

            if i < fieldnum:
                values[field.name] = field.get_value(dateval)
                i += 1
            elif i > fieldnum:
                values[field.name] = field.get_min(dateval)
                i += 1
            else:
                value = field.get_value(dateval)
                maxval = field.get_max(dateval)
                if value == maxval:
                    fieldnum -= 1
                    i -= 1
                else:
                    values[field.name] = value + 1
                    i += 1

        return datetime(**values), fieldnum

    def _set_field_value(self, dateval, fieldnum, new_value):
        values = {}
        for i, field in enumerate(self.fields):
            if field.REAL:
                if i < fieldnum:
                    values[field.name] = field.get_value(dateval)
                elif i > fieldnum:
                    values[field.name] = field.get_min(dateval)
                else:
                    values[field.name] = new_value

        return datetime(**values)

    def get_next_fire_time(self, start_date):
        if self.start_date:
            start_date = max(start_date, self.start_date)
        next_date = datetime_ceil(start_date)
        fieldnum = 0
        while 0 <= fieldnum < len(self.fields):
            field = self.fields[fieldnum]
            curr_value = field.get_value(next_date)
            next_value = field.get_next_value(next_date)

            if next_value is None:
                # No valid value was found
                next_date, fieldnum = self._increment_field_value(
                    next_date, fieldnum - 1)
            elif next_value > curr_value:
                # A valid, but higher than the starting value, was found
                if field.REAL:
                    next_date = self._set_field_value(
                        next_date, fieldnum, next_value)
                    fieldnum += 1
                else:
                    next_date, fieldnum = self._increment_field_value(
                        next_date, fieldnum)
            else:
                # A valid value was found, no changes necessary
                fieldnum += 1

        if fieldnum >= 0:
            return next_date

    def __str__(self):
        options = ["%s='%s'" % (f.name, str(f)) for f in self.fields
                   if not f.is_default]
        return 'cron[%s]' % (', '.join(options))

    def __repr__(self):
        options = ["%s='%s'" % (f.name, str(f)) for f in self.fields
                   if not f.is_default]
        if self.start_date:
            options.append("start_date='%s'" % self.start_date.isoformat(' '))
        return '<%s (%s)>' % (self.__class__.__name__, ', '.join(options))
