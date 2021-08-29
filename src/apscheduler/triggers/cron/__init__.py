from datetime import datetime, timedelta, tzinfo
from typing import ClassVar, List, Optional, Sequence, Tuple, Union

from ...abc import Trigger
from ...marshalling import marshal_date, marshal_timezone, unmarshal_date, unmarshal_timezone
from ...util import timezone_repr
from ...validators import as_aware_datetime, as_timezone, require_state_version
from .fields import (
    DEFAULT_VALUES, BaseField, DayOfMonthField, DayOfWeekField, MonthField, WeekField)


class CronTrigger(Trigger):
    """
    Triggers when current time matches all specified time constraints, similarly to how the UNIX
    cron scheduler works.

    :param year: 4-digit year
    :param month: month (1-12)
    :param day: day of the (1-31)
    :param week: ISO week (1-53)
    :param day_of_week: number or name of weekday (0-7 or sun,mon,tue,wed,thu,fri,sat,sun)
    :param hour: hour (0-23)
    :param minute: minute (0-59)
    :param second: second (0-59)
    :param start_time: earliest possible date/time to trigger on (defaults to current time)
    :param end_time: latest possible date/time to trigger on
    :param timezone: time zone to use for the date/time calculations
        (defaults to the local timezone)

    .. note:: The first weekday is always **monday**.
    """

    __slots__ = 'timezone', 'start_time', 'end_time', '_fields', '_last_fire_time'

    FIELDS_MAP: ClassVar[List] = [
        ('year', BaseField),
        ('month', MonthField),
        ('day', DayOfMonthField),
        ('week', WeekField),
        ('day_of_week', DayOfWeekField),
        ('hour', BaseField),
        ('minute', BaseField),
        ('second', BaseField)
    ]

    def __init__(self, *, year: Union[int, str, None] = None, month: Union[int, str, None] = None,
                 day: Union[int, str, None] = None, week: Union[int, str, None] = None,
                 day_of_week: Union[int, str, None] = None, hour: Union[int, str, None] = None,
                 minute: Union[int, str, None] = None, second: Union[int, str, None] = None,
                 start_time: Union[datetime, str, None] = None,
                 end_time: Union[datetime, str, None] = None,
                 timezone: Union[str, tzinfo, None] = None):
        self.timezone = as_timezone(timezone)
        self.start_time = (as_aware_datetime(start_time, self.timezone)
                           or datetime.now(self.timezone))
        self.end_time = as_aware_datetime(end_time, self.timezone)
        self._set_fields([year, month, day, week, day_of_week, hour, minute, second])
        self._last_fire_time: Optional[datetime] = None

    def _set_fields(self, values: Sequence[Union[int, str, None]]) -> None:
        self._fields = []
        assigned_values = {field_name: value
                           for (field_name, _), value in zip(self.FIELDS_MAP, values)
                           if value is not None}
        for field_name, field_class in self.FIELDS_MAP:
            exprs = assigned_values.pop(field_name, None)
            if exprs is None:
                exprs = '*' if assigned_values else DEFAULT_VALUES[field_name]

            field = field_class(field_name, exprs)
            self._fields.append(field)

    @classmethod
    def from_crontab(cls, expr: str, timezone: Union[str, tzinfo] = 'local') -> 'CronTrigger':
        """
        Create a :class:`~CronTrigger` from a standard crontab expression.

        See https://en.wikipedia.org/wiki/Cron for more information on the format accepted here.

        :param expr: minute, hour, day of month, month, day of week
        :param timezone: time zone to use for the date/time calculations
            (defaults to local timezone if omitted)

        """
        values = expr.split()
        if len(values) != 5:
            raise ValueError(f'Wrong number of fields; got {len(values)}, expected 5')

        return cls(minute=values[0], hour=values[1], day=values[2], month=values[3],
                   day_of_week=values[4], timezone=timezone)

    def _increment_field_value(self, dateval: datetime, fieldnum: int) -> Tuple[datetime, int]:
        """
        Increments the designated field and resets all less significant fields to their minimum
        values.

        :return: a tuple containing the new date, and the number of the field that was actually
            incremented
        """

        values = {}
        i = 0
        while i < len(self._fields):
            field = self._fields[i]
            if not field.real:
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

        difference = datetime(**values) - dateval.replace(tzinfo=None)
        dateval = datetime.fromtimestamp(dateval.timestamp() + difference.total_seconds(),
                                         self.timezone)
        return dateval, fieldnum
        # return datetime_normalize(dateval + difference), fieldnum

    def _set_field_value(self, dateval, fieldnum, new_value):
        values = {}
        for i, field in enumerate(self._fields):
            if field.real:
                if i < fieldnum:
                    values[field.name] = field.get_value(dateval)
                elif i > fieldnum:
                    values[field.name] = field.get_min(dateval)
                else:
                    values[field.name] = new_value

        return datetime(**values, tzinfo=self.timezone)

    def next(self) -> Optional[datetime]:
        if self._last_fire_time:
            start_time = self._last_fire_time + timedelta(microseconds=1)
        else:
            start_time = self.start_time

        fieldnum = 0
        next_time = datetime_ceil(start_time).astimezone(self.timezone)
        while 0 <= fieldnum < len(self._fields):
            field = self._fields[fieldnum]
            curr_value = field.get_value(next_time)
            next_value = field.get_next_value(next_time)

            if next_value is None:
                # No valid value was found
                next_time, fieldnum = self._increment_field_value(next_time, fieldnum - 1)
            elif next_value > curr_value:
                # A valid, but higher than the starting value, was found
                if field.real:
                    next_time = self._set_field_value(next_time, fieldnum, next_value)
                    fieldnum += 1
                else:
                    next_time, fieldnum = self._increment_field_value(next_time, fieldnum)
            else:
                # A valid value was found, no changes necessary
                fieldnum += 1

            # Return if the date has rolled past the end date
            if self.end_time and next_time > self.end_time:
                return None

        if fieldnum >= 0:
            self._last_fire_time = next_time
            return next_time

    def __getstate__(self):
        return {
            'version': 1,
            'timezone': marshal_timezone(self.timezone),
            'fields': [str(f) for f in self._fields],
            'start_time': marshal_date(self.start_time),
            'end_time': marshal_date(self.end_time),
            'last_fire_time': marshal_date(self._last_fire_time)
        }

    def __setstate__(self, state):
        require_state_version(self, state, 1)
        self.timezone = unmarshal_timezone(state['timezone'])
        self.start_time = unmarshal_date(state['start_time'])
        self.end_time = unmarshal_date(state['end_time'])
        self._last_fire_time = unmarshal_date(state['last_fire_time'])
        self._set_fields(state['fields'])

    def __repr__(self):
        fields = [f'{field.name}={str(field)!r}' for field in self._fields]
        fields.append(f'start_time={self.start_time.isoformat()!r}')
        if self.end_time:
            fields.append(f'end_time={self.end_time.isoformat()!r}')

        fields.append(f'timezone={timezone_repr(self.timezone)!r}')
        return f'CronTrigger({", ".join(fields)})'


def datetime_ceil(dateval: datetime) -> datetime:
    """Round the given datetime object upwards."""
    if dateval.microsecond > 0:
        return dateval + timedelta(seconds=1, microseconds=-dateval.microsecond)

    return dateval
