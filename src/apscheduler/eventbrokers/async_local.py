from __future__ import annotations

from inspect import isawaitable
from logging import Logger, getLogger
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
    _logger: Logger = attr.field(init=False, factory=lambda: getLogger(__name__))
    _task_group: TaskGroup = attr.field(init=False)

    async def __aenter__(self) -> LocalAsyncEventBroker:
        self._task_group = create_task_group()
        await self._task_group.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._task_group.__aexit__(exc_type, exc_val, exc_tb)
        del self._task_group

    async def publish(self, event: Event) -> None:
        async def deliver_event(func: Callable[[Event], Any]) -> None:
            try:
                retval = func(event)
                if isawaitable(retval):
                    await retval
            except BaseException:
                self._logger.exception('Error delivering %s event', event.__class__.__name__)

        event_type = type(event)
        for subscription in self._subscriptions.values():
            if subscription.event_types is None or event_type in subscription.event_types:
                self._task_group.start_soon(deliver_event, subscription.callback)
