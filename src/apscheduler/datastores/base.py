from __future__ import annotations

from apscheduler.abc import (
    AsyncDataStore,
    AsyncEventBroker,
    DataStore,
    EventBroker,
    EventSource,
)


class BaseDataStore(DataStore):
    _events: EventBroker

    def start(self, event_broker: EventBroker) -> None:
        self._events = event_broker

    def stop(self, *, force: bool = False) -> None:
        del self._events

    @property
    def events(self) -> EventSource:
        return self._events


class BaseAsyncDataStore(AsyncDataStore):
    _events: AsyncEventBroker

    async def start(self, event_broker: AsyncEventBroker) -> None:
        self._events = event_broker

    async def stop(self, *, force: bool = False) -> None:
        del self._events

    @property
    def events(self) -> EventSource:
        return self._events
