from datetime import timedelta, datetime, date, time

from pytz.exceptions import AmbiguousTimeError, NonExistentTimeError

from apscheduler.triggers.base import BaseTrigger
from apscheduler.util import astimezone


class CalendarIntervalTrigger(BaseTrigger):
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
    :param start_date: starting point for the interval calculation (defaults to current date if
        omitted)
    :param end_date: latest possible date to trigger on
    :param timezone: time zone to use for calculating the next fire time
        (defaults to the scheduling's default time zone)
    """

    __slots__ = 'years', 'months', 'weeks', 'days', 'time', 'start_date', 'end_date', 'timezone'

    def __init__(self, *, years: int = 0, months: int = 0, weeks: int = 0, days: int = 0,
                 hour: int = 0, minute: int = 0, second: int = 0, start_date: date = None,
                 end_date: date = None, timezone=None) -> None:
        self.years = years
        self.months = months
        self.weeks = weeks
        self.days = days
        self.time = time(hour, minute, second)
        self.start_date = start_date
        self.end_date = end_date
        self.timezone = astimezone(timezone)

        if self.years == self.months == self.weeks == self.days == 0:
            raise ValueError('interval must be at least 1 day long')
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError('end_date cannot be earlier than start_date')

    def get_next_fire_time(self, previous_fire_time, now):
        # Determine the starting point of the calculations
        timezone = self.timezone or now.tzinfo
        previous_date = previous_fire_time.date() if previous_fire_time else None

        while True:
            if previous_date:
                year, month = previous_date.year, previous_date.month
                while True:
                    month += self.months
                    year += self.years + month // 12
                    month %= 12
                    try:
                        next_date = date(year, month, previous_date.day)
                    except ValueError:
                        pass  # Nonexistent date
                    else:
                        next_date += timedelta(self.days + self.weeks * 7)
                        break
            else:
                next_date = self.start_date if self.start_date else now.date()

            # Don't return any date past end_date
            if self.end_date and next_date > self.end_date:
                return None

            next_time = datetime.combine(next_date, self.time)
            try:
                return timezone.localize(next_time, is_dst=None)
            except AmbiguousTimeError:
                # Return the daylight savings occurrence of the datetime
                return timezone.localize(next_time, is_dst=True)
            except NonExistentTimeError:
                # This datetime does not exist (the DST shift jumps over it)
                previous_date = next_date

    def __getstate__(self):
        return {
            'version': 1,
            'interval': [self.years, self.months, self.weeks, self.days],
            'time': self.time,
            'start_date': self.start_date,
            'end_date': self.end_date,
            'timezone': str(self.timezone)
        }

    def __setstate__(self, state):
        if state.get('version', 1) > 1:
            raise ValueError(
                'Got serialized data for version %s of %s, but only version 1 can be handled' %
                (state['version'], self.__class__.__name__))

        self.years, self.months, self.weeks, self.days = state['interval']
        self.time = state['time']
        self.start_date = state['start_date']
        self.end_date = state['end_date']
        self.timezone = astimezone(state['timezone'])

    def __str__(self):
        options = []
        for field, suffix in [('years', 'y'), ('months', 'm'), ('weeks', 'w'), ('days', 'd')]:
            value = getattr(self, field)
            if value:
                options.append('{}{}'.format(value, suffix))

        return 'calendarinterval[{} at {}]'.format(', '.join(options), self.time)

    def __repr__(self):
        fields = 'years', 'months', 'weeks', 'days'
        interval_repr = ', '.join('%s=%d' % (attr, getattr(self, attr))
                                  for attr in fields if getattr(self, attr))
        return '{self.__class__.__name__}({}, time={self.time})'.format(interval_repr, self=self)
