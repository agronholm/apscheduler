from __future__ import annotations

from base64 import b64decode, b64encode
from collections.abc import Iterable
from contextlib import AsyncExitStack
from inspect import iscoroutine
from logging import Logger
from typing import Any, Callable

import attrs
from anyio import CapacityLimiter, create_task_group, to_thread
from anyio.abc import TaskGroup

from .. import _events
from .._events import Event
from .._exceptions import DeserializationError
from .._retry import RetryMixin
from ..abc import EventBroker, Serializer, Subscription
from ..serializers.json import JSONSerializer


@attrs.define(eq=False, frozen=True)
class LocalSubscription(Subscription):
    callback: Callable[[Event], Any]
    event_types: set[type[Event]] | None
    one_shot: bool
    is_async: bool
    token: object
    _source: BaseEventBroker

    def unsubscribe(self) -> None:
        self._source.unsubscribe(self.token)


@attrs.define(kw_only=True)
class BaseEventBroker(EventBroker):
    _logger: Logger = attrs.field(init=False)
    _subscriptions: dict[object, LocalSubscription] = attrs.field(
        init=False, factory=dict
    )
    _task_group: TaskGroup = attrs.field(init=False)
    _thread_limiter: CapacityLimiter = attrs.field(init=False)

    async def start(self, exit_stack: AsyncExitStack, logger: Logger) -> None:
        self._logger = logger
        self._task_group = await exit_stack.enter_async_context(create_task_group())
        self._thread_limiter = CapacityLimiter(1)

    def subscribe(
        self,
        callback: Callable[[Event], Any],
        event_types: Iterable[type[Event]] | None = None,
        *,
        is_async: bool = True,
        one_shot: bool = False,
    ) -> Subscription:
        types = set(event_types) if event_types else None
        token = object()
        subscription = LocalSubscription(
            callback, types, one_shot, is_async, token, self
        )
        self._subscriptions[token] = subscription
        return subscription

    def unsubscribe(self, token: object) -> None:
        self._subscriptions.pop(token, None)

    async def publish_local(self, event: Event) -> None:
        event_type = type(event)
        one_shot_tokens: list[object] = []
        for _token, subscription in self._subscriptions.items():
            if (
                subscription.event_types is None
                or event_type in subscription.event_types
            ):
                self._task_group.start_soon(self._deliver_event, subscription, event)
                if subscription.one_shot:
                    one_shot_tokens.append(subscription.token)

        for token in one_shot_tokens:
            self.unsubscribe(token)

    async def _deliver_event(
        self, subscription: LocalSubscription, event: Event
    ) -> None:
        try:
            if subscription.is_async:
                retval = subscription.callback(event)
                if iscoroutine(retval):
                    await retval
            else:
                await to_thread.run_sync(
                    subscription.callback, event, limiter=self._thread_limiter
                )
        except Exception:
            self._logger.exception(
                "Error delivering %s event", event.__class__.__name__
            )


@attrs.define(kw_only=True)
class BaseExternalEventBroker(BaseEventBroker, RetryMixin):
    """
    Base class for event brokers that use an external service.

    :param serializer: the serializer used to (de)serialize events for transport
    """

    serializer: Serializer = attrs.field(factory=JSONSerializer)

    def generate_notification(self, event: Event) -> bytes:
        serialized = self.serializer.serialize(event.marshal())
        return event.__class__.__name__.encode("ascii") + b" " + serialized

    def generate_notification_str(self, event: Event) -> str:
        serialized = self.serializer.serialize(event.marshal())
        return event.__class__.__name__ + " " + b64encode(serialized).decode("ascii")

    def _reconstitute_event(self, event_type: str, serialized: bytes) -> Event | None:
        try:
            kwargs = self.serializer.deserialize(serialized)
        except DeserializationError:
            self._logger.exception(
                "Failed to deserialize an event of type %s",
                event_type,
                extra={"serialized": serialized},
            )
            return None

        try:
            event_class = getattr(_events, event_type)
        except AttributeError:
            self._logger.error(
                "Receive notification for a nonexistent event type: %s",
                event_type,
                extra={"serialized": serialized},
            )
            return None

        try:
            return event_class.unmarshal(kwargs)
        except Exception:
            self._logger.exception("Error reconstituting event of type %s", event_type)
            return None

    def reconstitute_event(self, payload: bytes) -> Event | None:
        try:
            event_type_bytes, serialized = payload.split(b" ", 1)
        except ValueError:
            self._logger.error(
                "Received malformatted notification", extra={"payload": payload}
            )
            return None

        event_type = event_type_bytes.decode("ascii", errors="replace")
        return self._reconstitute_event(event_type, serialized)

    def reconstitute_event_str(self, payload: str) -> Event | None:
        try:
            event_type, b64_serialized = payload.split(" ", 1)
        except ValueError:
            self._logger.error(
                "Received malformatted notification", extra={"payload": payload}
            )
            return None

        return self._reconstitute_event(event_type, b64decode(b64_serialized))
