from datetime import datetime, tzinfo
from typing import Optional, Union

from dateutil.parser import parse

from ..abc import Trigger
from ..validators import require_state_version, as_aware_datetime, as_timezone


class DateTrigger(Trigger):
    """
    Triggers once on the given date/time.

    :param run_time: the date/time to run the job at
    :param timezone: time zone to use to convert ``run_time`` into a timezone aware datetime, if it
        isn't already (defaults to the local time zone)
    """

    __slots__ = 'run_time', '_completed'

    def __init__(self, run_time: datetime, timezone: Union[str, tzinfo, None] = None):
        timezone = as_timezone(timezone or run_time.tzinfo)
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
            'run_time': self.run_time.isoformat(),
            'completed': self._completed
        }

    def __setstate__(self, state):
        require_state_version(self, state, 1)
        self.run_time = parse(state['run_time'])
        self._completed = state['completed']

    def __repr__(self):
        return f'DateTrigger({self.run_time.isoformat()!r})'
