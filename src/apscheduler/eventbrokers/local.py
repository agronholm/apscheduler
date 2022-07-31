from __future__ import annotations

from asyncio import iscoroutinefunction
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from threading import Lock
from typing import Any, Callable, Iterable

import attrs

from .._events import Event
from ..abc import EventBroker, Subscription
from .base import BaseEventBroker


@attrs.define(eq=False)
class LocalEventBroker(EventBroker, BaseEventBroker):
    """
    Synchronous, local event broker.

    This event broker only broadcasts within the process it runs in, and is therefore
    not suitable for multi-node or multiprocess use cases.

    Does not serialize events.
    """

    _executor: ThreadPoolExecutor = attrs.field(init=False)
    _exit_stack: ExitStack = attrs.field(init=False)
    _subscriptions_lock: Lock = attrs.field(init=False, factory=Lock)

    def start(self) -> None:
        self._executor = ThreadPoolExecutor(1)

    def stop(self, *, force: bool = False) -> None:
        self._executor.shutdown(wait=not force)
        del self._executor

    def subscribe(
        self,
        callback: Callable[[Event], Any],
        event_types: Iterable[type[Event]] | None = None,
        *,
        one_shot: bool = False,
    ) -> Subscription:
        if iscoroutinefunction(callback):
            raise ValueError(
                "Coroutine functions are not supported as callbacks on a synchronous "
                "event source"
            )

        with self._subscriptions_lock:
            return super().subscribe(callback, event_types, one_shot=one_shot)

    def unsubscribe(self, token: object) -> None:
        with self._subscriptions_lock:
            super().unsubscribe(token)

    def publish(self, event: Event) -> None:
        self.publish_local(event)

    def publish_local(self, event: Event) -> None:
        event_type = type(event)
        with self._subscriptions_lock:
            one_shot_tokens: list[object] = []
            for _token, subscription in self._subscriptions.items():
                if (
                    subscription.event_types is None
                    or event_type in subscription.event_types
                ):
                    self._executor.submit(
                        self._deliver_event, subscription.callback, event
                    )
                    if subscription.one_shot:
                        one_shot_tokens.append(subscription.token)

            for token in one_shot_tokens:
                super().unsubscribe(token)

    def _deliver_event(self, func: Callable[[Event], Any], event: Event) -> None:
        try:
            func(event)
        except BaseException:
            self._logger.exception(
                "Error delivering %s event", event.__class__.__name__
            )
