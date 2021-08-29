from datetime import datetime, timezone
from functools import partial
from operator import setitem
from typing import List, Optional

import pytest
from _pytest.logging import LogCaptureFixture

from apscheduler.events import AsyncEventHub, Event, EventHub


class TestEventHub:
    def test_publish(self) -> None:
        timestamp = datetime.now(timezone.utc)
        events: List[Optional[Event]] = [None, None]
        with EventHub() as eventhub:
            eventhub.subscribe(partial(setitem, events, 0))
            eventhub.subscribe(partial(setitem, events, 1))
            eventhub.publish(Event(timestamp=timestamp))

        assert events[0] is events[1]
        assert isinstance(events[0], Event)
        assert events[0].timestamp == timestamp

    def test_unsubscribe(self) -> None:
        timestamp = datetime.now(timezone.utc)
        events = []
        with EventHub() as eventhub:
            token = eventhub.subscribe(events.append)
            eventhub.publish(Event(timestamp=timestamp))
            eventhub.unsubscribe(token)
            eventhub.publish(Event(timestamp=timestamp))

        assert len(events) == 1

    def test_publish_no_subscribers(self, caplog: LogCaptureFixture) -> None:
        with EventHub() as eventhub:
            eventhub.publish(Event(timestamp=datetime.now(timezone.utc)))

        assert not caplog.text

    def test_publish_exception(self, caplog: LogCaptureFixture) -> None:
        def bad_subscriber(event: Event) -> None:
            raise Exception('foo')

        timestamp = datetime.now(timezone.utc)
        events = []
        with EventHub() as eventhub:
            eventhub.subscribe(bad_subscriber)
            eventhub.subscribe(events.append)
            eventhub.publish(Event(timestamp=timestamp))

        assert isinstance(events[0], Event)
        assert events[0].timestamp == timestamp
        assert 'Error delivering Event' in caplog.text

    def test_subscribe_coroutine_callback(self) -> None:
        async def callback(event: Event) -> None:
            pass

        with EventHub() as eventhub:
            with pytest.raises(ValueError, match='Coroutine functions are not supported'):
                eventhub.subscribe(callback)

    def test_relay_events(self) -> None:
        timestamp = datetime.now(timezone.utc)
        events = []
        with EventHub() as eventhub1, EventHub() as eventhub2:
            eventhub2.relay_events_from(eventhub1)
            eventhub2.subscribe(events.append)
            eventhub1.publish(Event(timestamp=timestamp))

        assert isinstance(events[0], Event)
        assert events[0].timestamp == timestamp


@pytest.mark.anyio
class TestAsyncEventHub:
    async def test_publish(self) -> None:
        async def async_setitem(event: Event) -> None:
            events[1] = event

        timestamp = datetime.now(timezone.utc)
        events: List[Optional[Event]] = [None, None]
        async with AsyncEventHub() as eventhub:
            eventhub.subscribe(partial(setitem, events, 0))
            eventhub.subscribe(async_setitem)
            eventhub.publish(Event(timestamp=timestamp))

        assert events[0] is events[1]
        assert isinstance(events[0], Event)
        assert events[0].timestamp == timestamp

    async def test_unsubscribe(self) -> None:
        timestamp = datetime.now(timezone.utc)
        events = []
        async with AsyncEventHub() as eventhub:
            token = eventhub.subscribe(events.append)
            eventhub.publish(Event(timestamp=timestamp))
            eventhub.unsubscribe(token)
            eventhub.publish(Event(timestamp=timestamp))

        assert len(events) == 1

    async def test_publish_no_subscribers(self, caplog: LogCaptureFixture) -> None:
        async with AsyncEventHub() as eventhub:
            eventhub.publish(Event(timestamp=datetime.now(timezone.utc)))

        assert not caplog.text

    async def test_publish_exception(self, caplog: LogCaptureFixture) -> None:
        def bad_subscriber(event: Event) -> None:
            raise Exception('foo')

        timestamp = datetime.now(timezone.utc)
        events = []
        async with AsyncEventHub() as eventhub:
            eventhub.subscribe(bad_subscriber)
            eventhub.subscribe(events.append)
            eventhub.publish(Event(timestamp=timestamp))

        assert isinstance(events[0], Event)
        assert events[0].timestamp == timestamp
        assert 'Error delivering Event' in caplog.text

    async def test_relay_events(self) -> None:
        timestamp = datetime.now(timezone.utc)
        events = []
        async with AsyncEventHub() as eventhub1, AsyncEventHub() as eventhub2:
            eventhub1.relay_events_from(eventhub2)
            eventhub1.subscribe(events.append)
            eventhub2.publish(Event(timestamp=timestamp))

        assert isinstance(events[0], Event)
        assert events[0].timestamp == timestamp
