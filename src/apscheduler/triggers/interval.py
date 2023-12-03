from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import attrs
from attr.validators import instance_of, optional

from .._converters import as_aware_datetime
from .._utils import require_state_version
from ..abc import Trigger


@attrs.define(kw_only=True)
class IntervalTrigger(Trigger):
    """
    Triggers on specified intervals.

    The first trigger time is on ``start_time`` which is the  moment the trigger was
    created unless specifically overridden. If ``end_time`` is specified, the last
    trigger time will be at or before that time. If no ``end_time`` has been given, the
    trigger will produce new trigger times as long as the resulting datetimes are valid
    datetimes in Python.

    :param weeks: number of weeks to wait
    :param days: number of days to wait
    :param hours: number of hours to wait
    :param minutes: number of minutes to wait
    :param seconds: number of seconds to wait
    :param microseconds: number of microseconds to wait
    :param start_time: first trigger date/time (defaults to current date/time if
        omitted)
    :param end_time: latest possible date/time to trigger on
    """

    weeks: float = 0
    days: float = 0
    hours: float = 0
    minutes: float = 0
    seconds: float = 0
    microseconds: float = 0
    start_time: datetime = attrs.field(
        converter=as_aware_datetime,
        factory=datetime.now,
        validator=instance_of(datetime),
    )
    end_time: datetime | None = attrs.field(
        converter=as_aware_datetime,
        validator=optional(instance_of(datetime)),
        default=None,
    )
    _interval: timedelta = attrs.field(init=False, eq=False, repr=False)
    _last_fire_time: datetime | None = attrs.field(
        init=False, eq=False, converter=as_aware_datetime, default=None
    )

    def __attrs_post_init__(self) -> None:
        self._interval = timedelta(
            weeks=self.weeks,
            days=self.days,
            hours=self.hours,
            minutes=self.minutes,
            seconds=self.seconds,
            microseconds=self.microseconds,
        )

        if self._interval.total_seconds() <= 0:
            raise ValueError("The time interval must be positive")

        if self.end_time and self.end_time < self.start_time:
            raise ValueError("end_time cannot be earlier than start_time")

    def next(self) -> datetime | None:
        if self._last_fire_time is None:
            self._last_fire_time = self.start_time
        else:
            self._last_fire_time += self._interval

        if self.end_time is None or self._last_fire_time <= self.end_time:
            return self._last_fire_time
        else:
            return None

    def __getstate__(self) -> dict[str, Any]:
        return {
            "version": 1,
            "interval": [
                self.weeks,
                self.days,
                self.hours,
                self.minutes,
                self.seconds,
                self.microseconds,
            ],
            "start_time": self.start_time,
            "end_time": self.end_time,
            "last_fire_time": self._last_fire_time,
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        require_state_version(self, state, 1)
        (
            self.weeks,
            self.days,
            self.hours,
            self.minutes,
            self.seconds,
            self.microseconds,
        ) = state["interval"]
        self.start_time = state["start_time"]
        self.end_time = state["end_time"]
        self._last_fire_time = state["last_fire_time"]
        self._interval = timedelta(
            weeks=self.weeks,
            days=self.days,
            hours=self.hours,
            minutes=self.minutes,
            seconds=self.seconds,
            microseconds=self.microseconds,
        )

    def __repr__(self) -> str:
        fields = []
        for field in "weeks", "days", "hours", "minutes", "seconds", "microseconds":
            value = getattr(self, field)
            if value > 0:
                fields.append(f"{field}={value}")

        fields.append(f"start_time='{self.start_time}'")
        if self.end_time:
            fields.append(f"end_time='{self.end_time}'")

        return f'{self.__class__.__name__}({", ".join(fields)})'
