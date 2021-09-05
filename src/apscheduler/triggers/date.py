from __future__ import annotations

from datetime import datetime, tzinfo
from typing import Optional

from ..abc import Trigger
from ..marshalling import marshal_date, unmarshal_date
from ..validators import as_aware_datetime, as_timezone, require_state_version


class DateTrigger(Trigger):
    """
    Triggers once on the given date/time.

    :param run_time: the date/time to run the job at
    :param timezone: time zone to use to convert ``run_time`` into a timezone aware datetime, if it
        isn't already
    """

    __slots__ = 'run_time', '_completed'

    def __init__(self, run_time: datetime, timezone: tzinfo | str = 'local'):
        timezone = as_timezone(timezone)
        self.run_time = as_aware_datetime(run_time, timezone)
        self._completed = False

    def next(self) -> Optional[datetime]:
        if not self._completed:
            self._completed = True
            return self.run_time
        else:
            return None

    def __getstate__(self):
        return {
            'version': 1,
            'run_time': marshal_date(self.run_time),
            'completed': self._completed
        }

    def __setstate__(self, state):
        require_state_version(self, state, 1)
        self.run_time = unmarshal_date(state['run_time'])
        self._completed = state['completed']

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.run_time}')"
