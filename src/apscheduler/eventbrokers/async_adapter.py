from __future__ import annotations

from contextlib import AsyncExitStack
from functools import partial
from typing import Any, Callable, Iterable, Optional

import attr
from anyio import to_thread
from anyio.from_thread import BlockingPortal

from apscheduler.abc import EventBroker
from apscheduler.eventbrokers.async_local import LocalAsyncEventBroker
from apscheduler.events import Event, SubscriptionToken
from apscheduler.util import reentrant


@reentrant
@attr.define(eq=False)
class AsyncEventBrokerAdapter(LocalAsyncEventBroker):
    original: EventBroker
    portal: BlockingPortal
    _exit_stack: AsyncExitStack = attr.field(init=False)

    async def __aenter__(self):
        self._exit_stack = AsyncExitStack()
        if not self.portal:
            self.portal = BlockingPortal()
            self._exit_stack.enter_async_context(self.portal)

        await to_thread.run_sync(self.original.__enter__)
        return await super().__aenter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await to_thread.run_sync(self.original.__exit__, exc_type, exc_val, exc_tb)
        await self._exit_stack.__aexit__(exc_type, exc_val, exc_tb)
        return await super().__aexit__(exc_type, exc_val, exc_tb)

    async def publish(self, event: Event) -> None:
        await to_thread.run_sync(self.original.publish, event)

    def subscribe(self, callback: Callable[[Event], Any],
                  event_types: Optional[Iterable[type[Event]]] = None) -> SubscriptionToken:
        token = self.original.subscribe(partial(self.portal.call, callback), event_types)
        return token
