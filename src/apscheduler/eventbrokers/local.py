from __future__ import annotations

import attrs

from .._events import Event
from .._utils import create_repr
from .base import BaseEventBroker


@attrs.define(eq=False, repr=False)
class LocalEventBroker(BaseEventBroker):
    """
    Asynchronous, local event broker.

    This event broker only broadcasts within the process it runs in, and is therefore
    not suitable for multi-node or multiprocess use cases.

    Does not serialize events.
    """

    def __repr__(self) -> str:
        return create_repr(self)

    async def publish(self, event: Event) -> None:
        await self.publish_local(event)
