from __future__ import annotations

import attrs

from .._events import Event
from .base import BaseEventBroker


@attrs.define(eq=False)
class LocalEventBroker(BaseEventBroker):
    """
    Asynchronous, local event broker.

    This event broker only broadcasts within the process it runs in, and is therefore
    not suitable for multi-node or multiprocess use cases.

    Does not serialize events.
    """

    async def publish(self, event: Event) -> None:
        await self.publish_local(event)
