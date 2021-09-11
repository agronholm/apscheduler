from __future__ import annotations

from asyncio import iscoroutinefunction
from concurrent.futures import ThreadPoolExecutor
from logging import Logger, getLogger
from typing import Any, Callable, Iterable, Optional

import attr

from ..events import Event, SubscriptionToken
from ..util import reentrant
from .base import BaseEventBroker


@reentrant
@attr.define(eq=False)
class LocalEventBroker(BaseEventBroker):
    _executor: ThreadPoolExecutor = attr.field(init=False)
    _logger: Logger = attr.field(init=False, factory=lambda: getLogger(__name__))

    def __enter__(self) -> LocalEventBroker:
        self._executor = ThreadPoolExecutor(1)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._executor.shutdown(wait=exc_type is None)
        del self._executor

    def subscribe(self, callback: Callable[[Event], Any],
                  event_types: Optional[Iterable[type[Event]]] = None) -> SubscriptionToken:
        if iscoroutinefunction(callback):
            raise ValueError('Coroutine functions are not supported as callbacks on a synchronous '
                             'event source')

        return super().subscribe(callback, event_types)

    def publish(self, event: Event) -> None:
        event_type = type(event)
        for subscription in list(self._subscriptions.values()):
            if subscription.event_types is None or event_type in subscription.event_types:
                self._executor.submit(self._deliver_event, subscription.callback, event)

    def _deliver_event(self, func: Callable[[Event], Any], event: Event) -> None:
        try:
            func(event)
        except BaseException:
            self._logger.exception('Error delivering %s event', event.__class__.__name__)
