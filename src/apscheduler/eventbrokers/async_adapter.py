from __future__ import annotations

from typing import Any, Callable, Iterable

import attrs
from anyio import to_thread
from anyio.from_thread import BlockingPortal

from .._events import Event
from ..abc import AsyncEventBroker, EventBroker, Subscription


@attrs.define(eq=False)
class AsyncEventBrokerAdapter(AsyncEventBroker):
    original: EventBroker

    async def start(self) -> None:
        await to_thread.run_sync(self.original.start)

    async def stop(self, *, force: bool = False) -> None:
        await to_thread.run_sync(lambda: self.original.stop(force=force))

    async def publish_local(self, event: Event) -> None:
        await to_thread.run_sync(self.original.publish_local, event)

    async def publish(self, event: Event) -> None:
        await to_thread.run_sync(self.original.publish, event)

    def subscribe(
        self,
        callback: Callable[[Event], Any],
        event_types: Iterable[type[Event]] | None = None,
        *,
        one_shot: bool = False,
    ) -> Subscription:
        return self.original.subscribe(callback, event_types, one_shot=one_shot)


@attrs.define(eq=False)
class SyncEventBrokerAdapter(EventBroker):
    original: AsyncEventBroker
    portal: BlockingPortal

    def start(self) -> None:
        pass

    def stop(self, *, force: bool = False) -> None:
        pass

    def publish_local(self, event: Event) -> None:
        self.portal.call(self.original.publish_local, event)

    def publish(self, event: Event) -> None:
        self.portal.call(self.original.publish, event)

    def subscribe(
        self,
        callback: Callable[[Event], Any],
        event_types: Iterable[type[Event]] | None = None,
        *,
        one_shot: bool = False,
    ) -> Subscription:
        return self.portal.call(
            lambda: self.original.subscribe(callback, event_types, one_shot=one_shot)
        )
