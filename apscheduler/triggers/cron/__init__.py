from datetime import datetime
from dateutil.tz import tzlocal

from six import iteritems

from apscheduler.triggers.base import BaseTrigger
from apscheduler.triggers.cron.fields import *
from apscheduler.util import datetime_ceil, convert_to_datetime, datetime_repr, astimezone


class CronTrigger(BaseTrigger):
    FIELD_NAMES = ('year', 'month', 'day', 'week', 'day_of_week', 'hour', 'minute', 'second')
    FIELDS_MAP = {
        'year': BaseField,
        'month': BaseField,
        'week': WeekField,
        'day': DayOfMonthField,
        'day_of_week': DayOfWeekField,
        'hour': BaseField,
        'minute': BaseField,
        'second': BaseField
    }

    def __init__(self, year=None, month=None, day=None, week=None, day_of_week=None, hour=None, minute=None,
                 second=None, start_date=None, timezone=None):
        """
        Triggers when current time matches all specified time constraints, emulating the UNIX cron scheduler.

        :param year: year to run on
        :param month: month to run on
        :param day: day of month to run on
        :param week: week of the year to run on
        :param day_of_week: weekday to run on (0 = Monday)
        :param hour: hour to run on
        :param second: second to run on
        :param start_date: earliest possible date/time to trigger on
        :param timezone: time zone for ``start_date``
        :type timezone: str or an instance of a :cls:`~datetime.tzinfo` subclass
        """

        self.timezone = astimezone(timezone) or getattr(start_date, 'tzinfo', None) or tzlocal()
        self.start_date = convert_to_datetime(start_date, self.timezone, 'start_date') if start_date else None

        values = dict((key, value) for (key, value) in iteritems(locals())
                      if key in self.FIELD_NAMES and value is not None)
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
        Increments the designated field and resets all less significant fields to their minimum values.

        :type dateval: datetime
        :type fieldnum: int
        :type amount: int
        :rtype: tuple
        :return: a tuple containing the new date, and the number of the field that was actually incremented
        """
        # Make sure to respect the timezone of the dateval parameter.
        values = {'tzinfo': dateval.tzinfo}

        i = 0
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
        values = {'tzinfo': dateval.tzinfo}
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

        # Make sure the timezone of next_date is in the timezone that
        # the CronTrigger's field values are in.
        next_date = next_date.astimezone(self.timezone)

        fieldnum = 0
        while 0 <= fieldnum < len(self.fields):
            field = self.fields[fieldnum]
            curr_value = field.get_value(next_date)
            next_value = field.get_next_value(next_date)

            if next_value is None:
                # No valid value was found
                next_date, fieldnum = self._increment_field_value(next_date, fieldnum - 1)
            elif next_value > curr_value:
                # A valid, but higher than the starting value, was found
                if field.REAL:
                    next_date = self._set_field_value(next_date, fieldnum, next_value)
                    fieldnum += 1
                else:
                    next_date, fieldnum = self._increment_field_value(next_date, fieldnum)
            else:
                # A valid value was found, no changes necessary
                fieldnum += 1

        if fieldnum >= 0:
            # Make sure that the returned date is in the trigger
            # timezone. Also, has the additional benefit of normalizing
            # the returned datetime.
            return next_date.astimezone(self.timezone)

    def __str__(self):
        options = ["%s='%s'" % (f.name, str(f)) for f in self.fields if not f.is_default]
        return 'cron[%s]' % (', '.join(options))

    def __repr__(self):
        options = ["%s='%s'" % (f.name, str(f)) for f in self.fields if not f.is_default]
        if self.start_date:
            options.append("start_date='%s'" % datetime_repr(self.start_date))
        return '<%s (%s)>' % (self.__class__.__name__, ', '.join(options))
