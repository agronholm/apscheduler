from __future__ import annotations

from functools import partial

import attr
from anyio import to_thread
from anyio.from_thread import BlockingPortal

from apscheduler.abc import EventBroker
from apscheduler.eventbrokers.async_local import LocalAsyncEventBroker
from apscheduler.events import Event
from apscheduler.util import reentrant


@reentrant
@attr.define(eq=False)
class AsyncEventBrokerAdapter(LocalAsyncEventBroker):
    original: EventBroker
    portal: BlockingPortal

    async def __aenter__(self):
        await super().__aenter__()

        if not self.portal:
            self.portal = BlockingPortal()
            self._exit_stack.enter_async_context(self.portal)

        await to_thread.run_sync(self.original.__enter__)
        self._exit_stack.push_async_exit(partial(to_thread.run_sync, self.original.__exit__))

        # Relay events from the original broker to this one
        self._exit_stack.enter_context(
            self.original.subscribe(partial(self.portal.call, self.publish_local))
        )

    async def publish(self, event: Event) -> None:
        await to_thread.run_sync(self.original.publish, event)
