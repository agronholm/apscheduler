from __future__ import annotations

from asyncio import iscoroutine
from typing import Any, Callable

import attrs
from anyio import create_task_group
from anyio.abc import TaskGroup

from .._events import Event
from ..abc import AsyncEventBroker
from .base import BaseEventBroker


@attrs.define(eq=False)
class LocalAsyncEventBroker(AsyncEventBroker, BaseEventBroker):
    """
    Asynchronous, local event broker.

    This event broker only broadcasts within the process it runs in, and is therefore
    not suitable for multi-node or multiprocess use cases.

    Does not serialize events.
    """

    _task_group: TaskGroup = attrs.field(init=False)

    async def start(self) -> None:
        self._task_group = create_task_group()
        await self._task_group.__aenter__()

    async def stop(self, *, force: bool = False) -> None:
        await self._task_group.__aexit__(None, None, None)
        del self._task_group

    async def publish(self, event: Event) -> None:
        await self.publish_local(event)

    async def publish_local(self, event: Event) -> None:
        event_type = type(event)
        one_shot_tokens: list[object] = []
        for _token, subscription in self._subscriptions.items():
            if (
                subscription.event_types is None
                or event_type in subscription.event_types
            ):
                self._task_group.start_soon(
                    self._deliver_event, subscription.callback, event
                )
                if subscription.one_shot:
                    one_shot_tokens.append(subscription.token)

        for token in one_shot_tokens:
            super().unsubscribe(token)

    async def _deliver_event(self, func: Callable[[Event], Any], event: Event) -> None:
        try:
            retval = func(event)
            if iscoroutine(retval):
                await retval
        except BaseException:
            self._logger.exception(
                "Error delivering %s event", event.__class__.__name__
            )
