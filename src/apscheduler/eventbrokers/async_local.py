from __future__ import annotations

from asyncio import iscoroutine
from contextlib import AsyncExitStack
from typing import Any, Callable

import attr
from anyio import create_task_group
from anyio.abc import TaskGroup

from ..abc import AsyncEventBroker
from ..events import Event
from ..util import reentrant
from .base import BaseEventBroker


@reentrant
@attr.define(eq=False)
class LocalAsyncEventBroker(AsyncEventBroker, BaseEventBroker):
    _task_group: TaskGroup = attr.field(init=False)
    _exit_stack: AsyncExitStack = attr.field(init=False)

    async def __aenter__(self) -> LocalAsyncEventBroker:
        self._exit_stack = AsyncExitStack()

        self._task_group = create_task_group()
        await self._exit_stack.enter_async_context(self._task_group)

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._exit_stack.__aexit__(exc_type, exc_val, exc_tb)
        del self._task_group

    async def publish(self, event: Event) -> None:
        await self.publish_local(event)

    async def publish_local(self, event: Event) -> None:
        event_type = type(event)
        one_shot_tokens: list[object] = []
        for token, subscription in self._subscriptions.items():
            if subscription.event_types is None or event_type in subscription.event_types:
                self._task_group.start_soon(self._deliver_event, subscription.callback, event)
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
            self._logger.exception('Error delivering %s event', event.__class__.__name__)
