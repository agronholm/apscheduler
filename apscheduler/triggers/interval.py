from datetime import timedelta, datetime, tzinfo
from typing import Optional, Union

from ..abc import Trigger
from ..validators import require_state_version, as_timezone, as_aware_datetime, as_timestamp


class IntervalTrigger(Trigger):
    """
    Triggers on specified intervals.

    The first trigger time is on ``start_time`` which is the  moment the trigger was created unless
    specifically overridden. If ``end_time`` is specified, the last trigger time will be at or
    before that time. If no ``end_time`` has been given, the trigger will produce new trigger times
    as long as the resulting datetimes are valid datetimes in Python.

    :param weeks: number of weeks to wait
    :param days: number of days to wait
    :param hours: number of hours to wait
    :param minutes: number of minutes to wait
    :param seconds: number of seconds to wait
    :param microseconds: number of microseconds to wait
    :param start_time: first trigger date/time
    :param end_time: latest possible date/time to trigger on
    :param timezone: time zone to use for normalizing calculated datetimes (defaults to the local
        timezone)
    """

    __slots__ = ('weeks', 'days', 'hours', 'minutes', 'seconds', 'microseconds', 'start_time',
                 'end_time', 'timezone', '_interval', '_last_fire_time')

    def __init__(self, *, weeks: int = 0, days: int = 0, hours: int = 0, minutes: int = 0,
                 seconds: int = 0, microseconds: int = 0, start_time: Optional[datetime] = None,
                 end_time: Optional[datetime] = None, timezone: Union[str, tzinfo, None] = None):
        self.weeks = weeks
        self.days = days
        self.hours = hours
        self.minutes = minutes
        self.seconds = seconds
        self.microseconds = microseconds
        self.timezone = as_timezone(timezone)
        self.start_time = as_aware_datetime(start_time or datetime.now(), self.timezone)
        self.end_time = as_aware_datetime(end_time, self.timezone)
        self._interval = timedelta(weeks=self.weeks, days=self.days, hours=self.hours,
                                   minutes=self.minutes, seconds=self.seconds,
                                   microseconds=self.microseconds)
        self._last_fire_time = None

        if self._interval.total_seconds() <= 0:
            raise ValueError('The time interval must be positive')

        if self.end_time and self.end_time < self.start_time:
            raise ValueError('end_time cannot be earlier than start_time')

    def next(self) -> Optional[datetime]:
        if self._last_fire_time is None:
            self._last_fire_time = self.start_time
        else:
            self._last_fire_time = self.timezone.normalize(self._last_fire_time + self._interval)

        if self.end_time is None or self._last_fire_time <= self.end_time:
            return self._last_fire_time
        else:
            return None

    def __getstate__(self):
        return {
            'version': 1,
            'interval': [self.weeks, self.days, self.hours, self.minutes, self.seconds,
                         self.microseconds],
            'timezone': self.timezone.zone,
            'start_time': as_timestamp(self.start_time),
            'end_time': as_timestamp(self.end_time),
            'last_fire_time': as_timestamp(self._last_fire_time)
        }

    def __setstate__(self, state):
        require_state_version(self, state, 1)
        self.weeks, self.days, self.hours, self.minutes, self.seconds, self.microseconds = \
            state['interval']
        self.timezone = as_timezone(state['timezone'])
        self.start_time = as_aware_datetime(state['start_time'], self.timezone)
        self.end_time = as_aware_datetime(state['end_time'], self.timezone)
        self._interval = timedelta(weeks=self.weeks, days=self.days, hours=self.hours,
                                   minutes=self.minutes, seconds=self.seconds,
                                   microseconds=self.microseconds)
        self._last_fire_time = as_aware_datetime(state['last_fire_time'], self.timezone)

    def __repr__(self):
        fields = []
        for field in 'weeks', 'days', 'hours', 'minutes', 'seconds', 'microseconds':
            value = getattr(self, field)
            if value > 0:
                fields.append(f'{field}={value}')

        fields.append(f'start_time={self.start_time.isoformat()!r}')
        if self.end_time:
            fields.append(f'end_time={self.end_time.isoformat()!r}')

        return f'IntervalTrigger({", ".join(fields)})'
