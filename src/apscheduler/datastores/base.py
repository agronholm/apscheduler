from __future__ import annotations

from contextlib import AsyncExitStack
from datetime import datetime, timezone
from logging import Logger
from typing import Iterable, Literal

import attrs

from .._enums import ConflictPolicy
from .._retry import RetryMixin
from .._structures import Schedule
from ..abc import DataStore, EventBroker, Serializer
from ..serializers.pickle import PickleSerializer


@attrs.define(kw_only=True)
class BaseDataStore(DataStore):
    """
    Base class for data stores.

    :param lock_expiration_delay: maximum amount of time (in seconds) that a scheduler
        can keep a lock on a schedule or task
    """

    lock_expiration_delay: float = 30
    _event_broker: EventBroker = attrs.field(init=False)
    _logger: Logger = attrs.field(init=False)

    async def start(
        self, exit_stack: AsyncExitStack, event_broker: EventBroker, logger: Logger
    ) -> None:
        self._event_broker = event_broker
        self._logger = logger

    async def pause_schedules(self, ids: Iterable[str]) -> None:
        for schedule in await self.get_schedules(ids):
            await self.add_schedule(
                attrs.evolve(schedule, paused=True),
                ConflictPolicy.replace,
            )

    def _get_unpaused_next_fire_time(
        self,
        schedule: Schedule,
        resume_from: datetime | Literal["now"] | None,
    ) -> datetime | None:
        if resume_from is None:
            return schedule.next_fire_time
        if resume_from == "now":
            resume_from = datetime.now(tz=timezone.utc)
        if (
            schedule.next_fire_time is not None
            and schedule.next_fire_time >= resume_from
        ):
            return schedule.next_fire_time
        try:
            while (next_fire_time := schedule.trigger.next()) < resume_from:
                pass  # Advance `next_fire_time` until its at or past `resume_from`
        except TypeError:  # The trigger is exhausted
            return None
        return next_fire_time

    async def unpause_schedules(
        self,
        ids: Iterable[str],
        *,
        resume_from: datetime | Literal["now"] | None = None,
    ) -> None:
        for schedule in await self.get_schedules(ids):
            await self.add_schedule(
                attrs.evolve(
                    schedule,
                    paused=False,
                    next_fire_time=self._get_unpaused_next_fire_time(
                        schedule,
                        resume_from,
                    ),
                ),
                ConflictPolicy.replace,
            )


@attrs.define(kw_only=True)
class BaseExternalDataStore(BaseDataStore, RetryMixin):
    """
    Base class for data stores using an external service such as a database.

    :param serializer: the serializer used to (de)serialize tasks, schedules and jobs
        for storage
    :param start_from_scratch: erase all existing data during startup (useful for test
        suites)
    """

    serializer: Serializer = attrs.field(factory=PickleSerializer)
    start_from_scratch: bool = attrs.field(default=False)
