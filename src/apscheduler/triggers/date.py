from __future__ import annotations

from datetime import datetime
from typing import Any

import attrs

from .._validators import as_aware_datetime, require_state_version
from ..abc import Trigger
from ..marshalling import marshal_date, unmarshal_date


@attrs.define
class DateTrigger(Trigger):
    """
    Triggers once on the given date/time.

    :param run_time: the date/time to run the job at
    """

    run_time: datetime = attrs.field(converter=as_aware_datetime)
    _completed: bool = attrs.field(init=False, eq=False, default=False)

    def next(self) -> datetime | None:
        if not self._completed:
            self._completed = True
            return self.run_time
        else:
            return None

    def __getstate__(self) -> dict[str, Any]:
        return {
            "version": 1,
            "run_time": marshal_date(self.run_time),
            "completed": self._completed,
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        require_state_version(self, state, 1)
        self.run_time = unmarshal_date(state["run_time"])
        self._completed = state["completed"]

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}('{self.run_time}')"
