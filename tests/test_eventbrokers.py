from __future__ import annotations

import sys
from contextlib import AsyncExitStack
from datetime import datetime, timezone
from logging import Logger

import pytest
from _pytest.logging import LogCaptureFixture
from anyio import CancelScope, create_memory_object_stream, fail_after

from apscheduler import Event, ScheduleAdded
from apscheduler.abc import EventBroker

if sys.version_info >= (3, 11):
    from datetime import UTC
else:
    UTC = timezone.utc

pytestmark = pytest.mark.anyio


async def test_publish_subscribe(event_broker: EventBroker) -> None:
    send, receive = create_memory_object_stream(2)
    event_broker.subscribe(send.send)
    event_broker.subscribe(send.send_nowait)
    event = ScheduleAdded(
        schedule_id="schedule1",
        task_id="task1",
        next_fire_time=datetime(2021, 9, 11, 12, 31, 56, 254867, UTC),
    )
    await event_broker.publish(event)

    with fail_after(3):
        event1 = await receive.receive()
        event2 = await receive.receive()

    assert event1 == event2
    assert isinstance(event1, ScheduleAdded)
    assert isinstance(event1.timestamp, datetime)
    assert event1.schedule_id == "schedule1"
    assert event1.task_id == "task1"
    assert event1.next_fire_time == datetime(2021, 9, 11, 12, 31, 56, 254867, UTC)


async def test_subscribe_one_shot(event_broker: EventBroker) -> None:
    send, receive = create_memory_object_stream(2)
    event_broker.subscribe(send.send, one_shot=True)
    event = ScheduleAdded(
        schedule_id="schedule1",
        task_id="task1",
        next_fire_time=datetime(2021, 9, 11, 12, 31, 56, 254867, UTC),
    )
    await event_broker.publish(event)
    event = ScheduleAdded(
        schedule_id="schedule2",
        task_id="task1",
        next_fire_time=datetime(2021, 9, 12, 8, 42, 11, 968481, UTC),
    )
    await event_broker.publish(event)

    with fail_after(3):
        received_event = await receive.receive()

    with pytest.raises(TimeoutError), fail_after(0.1):
        await receive.receive()

    assert isinstance(received_event, ScheduleAdded)
    assert received_event.schedule_id == "schedule1"
    assert received_event.task_id == "task1"


async def test_unsubscribe(event_broker: EventBroker) -> None:
    send, receive = create_memory_object_stream()
    subscription = event_broker.subscribe(send.send)
    await event_broker.publish(Event())
    with fail_after(3):
        await receive.receive()

    subscription.unsubscribe()
    await event_broker.publish(Event())
    with pytest.raises(TimeoutError), fail_after(0.1):
        await receive.receive()


async def test_publish_no_subscribers(
    event_broker: EventBroker, caplog: LogCaptureFixture
) -> None:
    await event_broker.publish(Event())
    assert not caplog.text


async def test_publish_exception(
    event_broker: EventBroker, caplog: LogCaptureFixture
) -> None:
    def bad_subscriber(event: Event) -> None:
        raise Exception("foo")

    timestamp = datetime.now(UTC)
    send, receive = create_memory_object_stream()
    event_broker.subscribe(bad_subscriber)
    event_broker.subscribe(send.send)
    await event_broker.publish(Event(timestamp=timestamp))

    received_event = await receive.receive()
    assert received_event.timestamp == timestamp
    assert "Error delivering Event" in caplog.text


async def test_cancel_start(raw_event_broker: EventBroker, logger: Logger) -> None:
    with CancelScope() as scope:
        scope.cancel()
        async with AsyncExitStack() as exit_stack:
            await raw_event_broker.start(exit_stack, logger)


async def test_cancel_stop(raw_event_broker: EventBroker, logger: Logger) -> None:
    with CancelScope() as scope:
        async with AsyncExitStack() as exit_stack:
            await raw_event_broker.start(exit_stack, logger)
            scope.cancel()
