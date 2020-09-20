from datetime import date, datetime, time, timedelta, tzinfo
from typing import Optional, Union

from pytz.exceptions import AmbiguousTimeError, NonExistentTimeError

from ..abc import Trigger
from ..validators import (
    as_aware_datetime, as_date, as_ordinal_date, as_timestamp, as_timezone, require_state_version)


class CalendarIntervalTrigger(Trigger):
    """
    Runs the task on specified calendar-based intervals always at the same exact time of day.

    When calculating the next date, the ``years`` and ``months`` parameters are first added to the
    previous date while keeping the day of the month constant. This is repeated until the resulting
    date is valid. After that, the ``weeks`` and ``days`` parameters are added to that date.
    Finally, the date is combined with the given time (hour, minute, second) to form the final
    datetime.

    This means that if the ``days`` or ``weeks`` parameters are not used, the task will always be
    executed on the same day of the month at the same wall clock time, assuming the date and time
    are valid.

    If the resulting datetime is invalid due to a daylight saving forward shift, the date is
    discarded and the process moves on to the next date. If instead the datetime is ambiguous due
    to a backward DST shift, the earlier of the two resulting datetimes is used.

    If no previous run time is specified when requesting a new run time (like when starting for the
    first time or resuming after being paused), ``start_date`` is used as a reference and the next
    valid datetime equal to or later than the current time will be returned. Otherwise, the next
    valid datetime starting from the previous run time is returned, even if it's in the past.

    .. warning:: Be wary of setting a start date near the end of the month (29. – 31.) if you have
        ``months`` specified in your interval, as this will skip the months where those days do not
        exist. Likewise, setting the start date on the leap day (February 29th) and having
        ``years`` defined may cause some years to be skipped.

        Users are also discouraged from  using a time inside the target timezone's DST switching
        period (typically around 2 am) since a date could either be skipped or repeated due to the
        specified wall clock time either occurring twice or not at all.

    :param years: number of years to wait
    :param months: number of months to wait
    :param weeks: number of weeks to wait
    :param days: number of days to wait
    :param hour: hour to run the task at
    :param minute: minute to run the task at
    :param second: second to run the task at
    :param start_date: first date to trigger on (defaults to current date if omitted)
    :param end_date: latest possible date to trigger on
    :param timezone: time zone to use for calculating the next fire time
    """

    __slots__ = ('years', 'months', 'weeks', 'days', 'start_date', 'end_date', 'timezone', '_time',
                 '_last_fire_date')

    def __init__(self, *, years: int = 0, months: int = 0, weeks: int = 0, days: int = 0,
                 hour: int = 0, minute: int = 0, second: int = 0,
                 start_date: Union[date, str, None] = None,
                 end_date: Union[date, str, None] = None,
                 timezone: Union[str, tzinfo, None] = None):
        self.years = years
        self.months = months
        self.weeks = weeks
        self.days = days
        self.timezone = as_timezone(timezone)
        self.start_date = as_date(start_date) or datetime.now(self.timezone).date()
        self.end_date = as_date(end_date)
        self._time = time(hour, minute, second)
        self._last_fire_date: Optional[date] = None

        if self.years == self.months == self.weeks == self.days == 0:
            raise ValueError('interval must be at least 1 day long')

        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError('end_date cannot be earlier than start_date')

    def next(self) -> Optional[datetime]:
        previous_date: date = self._last_fire_date
        while True:
            if previous_date:
                year, month = previous_date.year, previous_date.month
                while True:
                    month += self.months
                    year += self.years + (month - 1) // 12
                    month = (month - 1) % 12 + 1
                    try:
                        next_date = date(year, month, previous_date.day)
                    except ValueError:
                        pass  # Nonexistent date
                    else:
                        next_date += timedelta(self.days + self.weeks * 7)
                        break
            else:
                next_date = self.start_date

            # Don't return any date past end_date
            if self.end_date and next_date > self.end_date:
                return None

            next_time = datetime.combine(next_date, self._time)
            try:
                self._last_fire_date = next_date
                return self.timezone.localize(next_time, is_dst=None)
            except AmbiguousTimeError:
                # Return the daylight savings occurrence of the datetime
                return self.timezone.localize(next_time, is_dst=True)
            except NonExistentTimeError:
                # This datetime does not exist (the DST shift jumps over it)
                previous_date = next_date

    def __getstate__(self):
        return {
            'version': 1,
            'interval': [self.years, self.months, self.weeks, self.days],
            'time': [self._time.hour, self._time.minute, self._time.second],
            'start_date': as_ordinal_date(self.start_date),
            'end_date': as_ordinal_date(self.end_date),
            'timezone': self.timezone.zone,
            'last_fire_date': as_timestamp(self._last_fire_date)
        }

    def __setstate__(self, state):
        require_state_version(self, state, 1)
        self.years, self.months, self.weeks, self.days = state['interval']
        self.start_date = as_date(state['start_date'])
        self.end_date = as_date(state['end_date'])
        self.timezone = as_timezone(state['timezone'])
        self._time = time(*state['time'])
        self._last_fire_date = as_aware_datetime(state['last_fire_date'], self.timezone)

    def __repr__(self):
        fields = []
        for field in 'years', 'months', 'weeks', 'days':
            value = getattr(self, field)
            if value > 0:
                fields.append(f'{field}={value}')

        fields.append(f'time={self._time.isoformat()!r}')
        fields.append(f'start_date={self.start_date.isoformat()!r}')
        if self.end_date:
            fields.append(f'end_date={self.end_date.isoformat()!r}')

        fields.append(f'timezone={self.timezone.tzname(None)!r}')
        return f'CalendarIntervalTrigger({", ".join(fields)})'
