from __future__ import annotations

from datetime import date, datetime, time, timedelta, tzinfo
from typing import Any

import attrs
from attr.validators import instance_of, optional

from .._converters import as_aware_datetime, as_date, as_timezone
from .._utils import require_state_version, timezone_repr
from ..abc import Trigger


@attrs.define(kw_only=True)
class CalendarIntervalTrigger(Trigger):
    """
    Runs the task on specified calendar-based intervals always at the same exact time of
    day.

    When calculating the next date, the ``years`` and ``months`` parameters are first
    added to the previous date while keeping the day of the month constant. This is
    repeated until the resulting date is valid. After that, the ``weeks`` and ``days``
    parameters are added to that date. Finally, the date is combined with the given time
    (hour, minute, second) to form the final datetime.

    This means that if the ``days`` or ``weeks`` parameters are not used, the task will
    always be executed on the same day of the month at the same wall clock time,
    assuming the date and time are valid.

    If the resulting datetime is invalid due to a daylight saving forward shift, the
    date is discarded and the process moves on to the next date. If instead the datetime
    is ambiguous due to a backward DST shift, the earlier of the two resulting datetimes
    is used.

    If no previous run time is specified when requesting a new run time (like when
    starting for the first time or resuming after being paused), ``start_date`` is used
    as a reference and the next valid datetime equal to or later than the current time
    will be returned. Otherwise, the next valid datetime starting from the previous run
    time is returned, even if it's in the past.

    .. warning:: Be wary of setting a start date near the end of the month (29. – 31.)
        if you have ``months`` specified in your interval, as this will skip the months
        when those days do not exist. Likewise, setting the start date on the leap day
        (February 29th) and having ``years`` defined may cause some years to be skipped.

        Users are also discouraged from  using a time inside the target timezone's DST
        switching period (typically around 2 am) since a date could either be skipped or
        repeated due to the specified wall clock time either occurring twice or not at
        all.

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

    years: int = 0
    months: int = 0
    weeks: int = 0
    days: int = 0
    hour: int = 0
    minute: int = 0
    second: int = 0
    start_date: date = attrs.field(
        converter=as_date, validator=instance_of(date), factory=date.today
    )
    end_date: date | None = attrs.field(
        converter=as_date, validator=optional(instance_of(date)), default=None
    )
    timezone: tzinfo = attrs.field(
        converter=as_timezone, validator=instance_of(tzinfo), default="local"
    )
    _time: time = attrs.field(init=False, eq=False)
    _last_fire_date: date | None = attrs.field(
        init=False, eq=False, converter=as_aware_datetime, default=None
    )

    def __attrs_post_init__(self) -> None:
        self._time = time(self.hour, self.minute, self.second, tzinfo=self.timezone)

        if self.years == self.months == self.weeks == self.days == 0:
            raise ValueError("interval must be at least 1 day long")

        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError("end_date cannot be earlier than start_date")

    def next(self) -> datetime | None:
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

            # Combine the date with the designated time and normalize the result
            timestamp = datetime.combine(next_date, self._time).timestamp()
            next_time = datetime.fromtimestamp(timestamp, self.timezone)

            # Check if the time is off due to normalization and a forward DST shift
            if next_time.time() != self._time:
                previous_date = next_time.date()
            else:
                self._last_fire_date = next_date
                return next_time

    def __getstate__(self) -> dict[str, Any]:
        return {
            "version": 1,
            "interval": [self.years, self.months, self.weeks, self.days],
            "time": [self._time.hour, self._time.minute, self._time.second],
            "start_date": self.start_date,
            "end_date": self.end_date,
            "timezone": self.timezone,
            "last_fire_date": self._last_fire_date,
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        require_state_version(self, state, 1)
        self.years, self.months, self.weeks, self.days = state["interval"]
        self.start_date = state["start_date"]
        self.end_date = state["end_date"]
        self.timezone = state["timezone"]
        self._time = time(*state["time"], tzinfo=self.timezone)
        self._last_fire_date = state["last_fire_date"]

    def __repr__(self) -> str:
        fields = []
        for field in "years", "months", "weeks", "days":
            value = getattr(self, field)
            if value > 0:
                fields.append(f"{field}={value}")

        fields.append(f"time={self._time.isoformat()!r}")
        fields.append(f"start_date='{self.start_date}'")
        if self.end_date:
            fields.append(f"end_date='{self.end_date}'")

        fields.append(f"timezone={timezone_repr(self.timezone)!r}")
        return f'{self.__class__.__name__}({", ".join(fields)})'
