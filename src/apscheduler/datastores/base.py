from __future__ import annotations

from contextlib import AsyncExitStack
from logging import Logger
from typing import Any

import attrs
from attrs.validators import instance_of

from .._exceptions import SerializationError
from .._retry import RetryMixin
from .._structures import Job, Schedule
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
    :param allow_invocation_overrides: allow writing per-invocation callable references;
        enable only after all schedulers and workers sharing the store understand the
        invocation envelope (see :ref:`classmethod-record-upgrade`)
    """

    serializer: Serializer = attrs.field(factory=PickleSerializer)
    start_from_scratch: bool = attrs.field(default=False)
    allow_invocation_overrides: bool = attrs.field(
        default=False, validator=instance_of(bool)
    )

    def _marshal_invocation(self, invocation: Job | Schedule) -> dict[str, Any]:
        # Guard direct store writes as well as scheduler submissions. Reading both
        # formats is unconditional; the opt-in only controls new envelope writes.
        if (
            getattr(invocation, "_func", None) is not None
            and not self.allow_invocation_overrides
        ):
            raise SerializationError(
                "Class-method invocations require allow_invocation_overrides=True "
                "after all schedulers and workers sharing the store have been upgraded"
            )

        return invocation.marshal(self.serializer)
