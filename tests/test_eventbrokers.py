from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack
from datetime import UTC, datetime
from functools import partial
from logging import Logger
from threading import get_ident

import pytest
from _pytest.logging import LogCaptureFixture
from anyio import CancelScope, create_memory_object_stream, fail_after, from_thread
from anyio import Event as AnyIOEvent
from anyio.lowlevel import checkpoint

from apscheduler import Event, ScheduleAdded
from apscheduler.abc import EventBroker

pytestmark = pytest.mark.anyio


@pytest.fixture
async def started_local_broker(
    local_broker: EventBroker, logger: Logger
) -> AsyncGenerator[EventBroker, None]:
    async with AsyncExitStack() as exit_stack:
        await local_broker.start(exit_stack, logger)
        yield local_broker


@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
@pytest.mark.parametrize("is_async", [True, False])
@pytest.mark.parametrize("wrap_partial", [True, False])
async def test_coroutine_callback_ignores_thread_flag(
    started_local_broker: EventBroker, is_async: bool, wrap_partial: bool
) -> None:
    loop_thread = get_ident()
    send, receive = create_memory_object_stream[Event](1)

    async def callback(event: Event) -> None:
        assert get_ident() == loop_thread
        await checkpoint()
        await send.send(event)

    with send, receive:
        started_local_broker.subscribe(
            partial(callback) if wrap_partial else callback, is_async=is_async
        )
        event = Event()
        await started_local_broker.publish(event)
        with fail_after(1):
            assert await receive.receive() == event


@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
@pytest.mark.parametrize("is_async", [True, False])
async def test_sync_callback_thread(
    started_local_broker: EventBroker, is_async: bool
) -> None:
    loop_thread = get_ident()
    callback_threads: list[int] = []
    completed = AnyIOEvent()

    def callback(event: Event) -> None:
        callback_threads.append(get_ident())
        if get_ident() == loop_thread:
            completed.set()
        else:
            from_thread.run_sync(completed.set)

    started_local_broker.subscribe(callback, is_async=is_async)
    await started_local_broker.publish(Event())
    with fail_after(1):
        await completed.wait()

    assert (callback_threads[0] == loop_thread) is is_async


async def test_publish_subscribe(event_broker: EventBroker) -> None:
    send, receive = create_memory_object_stream[Event](2)
    with send, receive:
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
    send, receive = create_memory_object_stream[Event](2)
    with send, receive:
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
    send, receive = create_memory_object_stream[Event]()
    with send, receive:
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
    send, receive = create_memory_object_stream[Event]()
    with send, receive:
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


def test_asyncpg_broker_from_async_engine() -> None:
    pytest.importorskip("asyncpg", reason="asyncpg is not installed")
    from sqlalchemy import URL
    from sqlalchemy.ext.asyncio import create_async_engine

    from apscheduler.eventbrokers.asyncpg import AsyncpgEventBroker

    url = URL(
        "postgresql+asyncpg",
        "myuser",
        "c /%@",
        "localhost",
        7654,
        "dbname",
        {"opt1": "foo", "opt2": "bar"},
    )
    engine = create_async_engine(url)
    broker = AsyncpgEventBroker.from_async_sqla_engine(engine)
    assert isinstance(broker, AsyncpgEventBroker)
    assert broker.dsn == (
        "postgresql://myuser:c %2F%25%40@localhost:7654/dbname?opt1=foo&opt2=bar"
    )


def test_psycopg_broker_from_async_engine() -> None:
    pytest.importorskip(
        "psycopg", exc_type=ImportError, reason="psycopg is not installed"
    )
    from sqlalchemy import URL
    from sqlalchemy.ext.asyncio import create_async_engine

    from apscheduler.eventbrokers.psycopg import PsycopgEventBroker

    url = URL(
        "postgresql+psycopg",
        "myuser",
        "c /%@",
        "localhost",
        7654,
        "dbname",
        {"opt1": "foo", "opt2": "bar"},
    )
    engine = create_async_engine(url)
    broker = PsycopgEventBroker.from_async_sqla_engine(engine)
    assert isinstance(broker, PsycopgEventBroker)
    assert broker.conninfo == (
        "postgresql://myuser:c %2F%25%40@localhost:7654/dbname?opt1=foo&opt2=bar"
    )
