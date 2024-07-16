from __future__ import annotations

from contextlib import AsyncExitStack
from logging import Logger

import attrs

from .._retry import RetryMixin
from ..abc import DataStore, EventBroker, Serializer
from ..serializers.pickle import PickleSerializer


@attrs.define(kw_only=True)
class BaseDataStore(DataStore):
    """Base class for data stores."""

    _event_broker: EventBroker = attrs.field(init=False)
    _logger: Logger = attrs.field(init=False)

    async def start(
        self, exit_stack: AsyncExitStack, event_broker: EventBroker, logger: Logger
    ) -> None:
        self._event_broker = event_broker
        self._logger = logger


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
