from __future__ import annotations

from abc import abstractmethod
from datetime import datetime, timedelta
from typing import Any

import attrs

from .._converters import as_aware_datetime, as_timedelta, list_converter
from .._exceptions import MaxIterationsReached
from .._marshalling import marshal_object, unmarshal_object
from .._utils import require_state_version
from ..abc import Trigger


@attrs.define
class BaseCombiningTrigger(Trigger):
    triggers: list[Trigger]
    _next_fire_times: list[datetime | None] = attrs.field(
        init=False, eq=False, converter=list_converter(as_aware_datetime), factory=list
    )

    def __getstate__(self) -> dict[str, Any]:
        return {
            "version": 1,
            "triggers": [marshal_object(trigger) for trigger in self.triggers],
            "next_fire_times": self._next_fire_times,
        }

    @abstractmethod
    def __setstate__(self, state: dict[str, Any]) -> None:
        self.triggers = [
            unmarshal_object(*trigger_state) for trigger_state in state["triggers"]
        ]
        self._next_fire_times = state["next_fire_times"]


@attrs.define
class AndTrigger(BaseCombiningTrigger):
    """
    Fires on times produced by the enclosed triggers whenever the fire times are within
    the given threshold.

    If the produced fire times are not within the given threshold of each other, the
    trigger(s) that produced the earliest fire time will be asked for their next fire
    time and the iteration is restarted. If instead all the triggers agree on a fire
    time, all the triggers are asked for their next fire times and the earliest of the
    previously produced fire times will be returned.

    This trigger will be finished when any of the enclosed trigger has finished.

    :param triggers: triggers to combine
    :param threshold: maximum time difference between the next fire times of the
        triggers in order for the earliest of them to be returned from :meth:`next` (in
        seconds, or as timedelta)
    :param max_iterations: maximum number of iterations of fire time calculations before
        giving up
    """

    threshold: timedelta = attrs.field(converter=as_timedelta, default=1)
    max_iterations: int | None = 10000

    def next(self) -> datetime | None:
        if not self._next_fire_times:
            # Fill out the fire times on the first run
            self._next_fire_times = [t.next() for t in self.triggers]

        for _ in range(self.max_iterations):
            # Find the earliest and latest fire times
            earliest_fire_time: datetime | None = None
            latest_fire_time: datetime | None = None
            for fire_time in self._next_fire_times:
                # If any of the fire times is None, this trigger is finished
                if fire_time is None:
                    return None

                if earliest_fire_time is None or earliest_fire_time > fire_time:
                    earliest_fire_time = fire_time

                if latest_fire_time is None or latest_fire_time < fire_time:
                    latest_fire_time = fire_time

            # Replace all the fire times that were within the threshold
            for i, _trigger in enumerate(self.triggers):
                if self._next_fire_times[i] - earliest_fire_time <= self.threshold:
                    self._next_fire_times[i] = self.triggers[i].next()

            # If all the fire times were within the threshold, return the earliest one
            if latest_fire_time - earliest_fire_time <= self.threshold:
                return earliest_fire_time
        else:
            raise MaxIterationsReached

    def __getstate__(self) -> dict[str, Any]:
        state = super().__getstate__()
        state["threshold"] = self.threshold
        state["max_iterations"] = self.max_iterations
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        require_state_version(self, state, 1)
        super().__setstate__(state)
        self.threshold = state["threshold"]
        self.max_iterations = state["max_iterations"]

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}({self.triggers}, "
            f"threshold={self.threshold.total_seconds()}, "
            f"max_iterations={self.max_iterations})"
        )


@attrs.define
class OrTrigger(BaseCombiningTrigger):
    """
    Fires on every fire time of every trigger in chronological order.
    If two or more triggers produce the same fire time, it will only be used once.

    This trigger will be finished when none of the enclosed triggers can produce any new
    fire times.

    :param triggers: triggers to combine
    """

    cooldown_period: timedelta = attrs.field(converter=as_timedelta, default=0)
    _last_fire_time: datetime | None = attrs.field(default=None, eq=False, init=False)
    max_iterations: int | None = 10000

    def _get_next_valid_fire_time(self) -> tuple[datetime | None, list[int]]:
        """
        Find the next valid fire time that respects the cooldown period.

        Raises:
            MaxIterationsReached: If the maximum number of iterations is reached

        Returns:
            A tuple of (fire_time, trigger_indices) where fire_time is the next valid
            fire time (or None if no valid time exists) and trigger_indices is a list
            of indices of triggers that produced this fire time.
        """
        for _ in range(self.max_iterations):
            earliest_time = min(
                (
                    fire_time
                    for fire_time in self._next_fire_times
                    if fire_time is not None
                ),
                default=None,
            )
            if earliest_time is None:
                return None, []

            # Find all triggers that produced this fire time
            trigger_indices = [
                i
                for i, fire_time in enumerate(self._next_fire_times)
                if fire_time == earliest_time
            ]

            # Check if we need to respect cooldown period
            if (
                self.cooldown_period > timedelta(0)
                and self._last_fire_time is not None
                and earliest_time - self._last_fire_time < self.cooldown_period
            ):
                # Get next fire times for all triggers that would have fired
                for i in trigger_indices:
                    self._next_fire_times[i] = self.triggers[i].next()
                continue

            return earliest_time, trigger_indices
        else:
            raise MaxIterationsReached

    def next(self) -> datetime | None:
        """
        Get the next fire time that respects the cooldown period.

        Returns:
            The next valid fire time, or None if no more fire times exist.
        """
        # Initialize fire times if needed
        if not self._next_fire_times:
            self._next_fire_times = [t.next() for t in self.triggers]
            self._last_fire_time = None

        # Get next valid fire time and affected triggers
        try:
            fire_time, trigger_indices = self._get_next_valid_fire_time()
        except RecursionError:
            raise MaxIterationsReached

        if fire_time is not None:
            # Update last fire time and get next fire times for triggered sources
            self._last_fire_time = fire_time
            for i in trigger_indices:
                self._next_fire_times[i] = self.triggers[i].next()

        return fire_time

    def __setstate__(self, state: dict[str, Any]) -> None:
        require_state_version(self, state, 1)
        super().__setstate__(state)
        self.cooldown_period = state["cooldown_period"]
        self._last_fire_time = state["last_fire_time"]
        self.max_iterations = state["max_iterations"]

    def __getstate__(self) -> dict[str, Any]:
        state = super().__getstate__()
        state["cooldown_period"] = self.cooldown_period
        state["last_fire_time"] = self._last_fire_time
        state["max_iterations"] = self.max_iterations
        return state

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}"
            f"({self.triggers}, cooldown_period={self.cooldown_period.total_seconds()}"
            f", max_iterations={self.max_iterations})"
        )
