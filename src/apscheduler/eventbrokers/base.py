from __future__ import annotations

from base64 import b64decode, b64encode
from logging import Logger, getLogger
from typing import Any, Callable, Iterable, Optional

import attr

from .. import abc, events
from ..abc import EventBroker, Serializer
from ..events import Event, Subscription, SubscriptionToken
from ..exceptions import DeserializationError


@attr.define(eq=False)
class BaseEventBroker(EventBroker):
    _logger: Logger = attr.field(init=False)
    _subscriptions: dict[SubscriptionToken, Subscription] = attr.field(init=False, factory=dict)

    def __attrs_post_init__(self) -> None:
        self._logger = getLogger(self.__class__.__module__)

    def subscribe(self, callback: Callable[[Event], Any],
                  event_types: Optional[Iterable[type[Event]]] = None) -> SubscriptionToken:
        types = set(event_types) if event_types else None
        token = SubscriptionToken(object())
        subscription = Subscription(callback, types)
        self._subscriptions[token] = subscription
        return token

    def unsubscribe(self, token: SubscriptionToken) -> None:
        self._subscriptions.pop(token, None)

    def relay_events_from(self, source: abc.EventSource) -> SubscriptionToken:
        return source.subscribe(self.publish)


class DistributedEventBrokerMixin:
    serializer: Serializer
    _logger: Logger

    def generate_notification(self, event: Event) -> bytes:
        serialized = self.serializer.serialize(attr.asdict(event))
        return event.__class__.__name__.encode('ascii') + b' ' + serialized

    def generate_notification_str(self, event: Event) -> str:
        serialized = self.serializer.serialize(attr.asdict(event))
        return event.__class__.__name__ + ' ' + b64encode(serialized).decode('ascii')

    def _reconstitute_event(self, event_type: str, serialized: bytes) -> Optional[Event]:
        try:
            kwargs = self.serializer.deserialize(serialized)
        except DeserializationError:
            self._logger.exception('Failed to deserialize an event of type %s', event_type,
                                   serialized=serialized)
            return None

        try:
            event_class = getattr(events, event_type)
        except AttributeError:
            self._logger.error('Receive notification for a nonexistent event type: %s',
                               event_type, serialized=serialized)
            return None

        try:
            return event_class(**kwargs)
        except Exception:
            self._logger.exception('Error reconstituting event of type %s', event_type)
            return None

    def reconstitute_event(self, payload: bytes) -> Optional[Event]:
        try:
            event_type_bytes, serialized = payload.split(b' ', 1)
        except ValueError:
            self._logger.error('Received malformatted notification', payload=payload)
            return None

        event_type = event_type_bytes.decode('ascii', errors='replace')
        return self._reconstitute_event(event_type, serialized)

    def reconstitute_event_str(self, payload: str) -> Optional[Event]:
        try:
            event_type, b64_serialized = payload.split(' ', 1)
        except ValueError:
            self._logger.error('Received malformatted notification', payload=payload)
            return None

        return self._reconstitute_event(event_type, b64decode(b64_serialized))
