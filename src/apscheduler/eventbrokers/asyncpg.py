from __future__ import annotations

from collections.abc import AsyncGenerator, Mapping
from contextlib import AsyncExitStack, asynccontextmanager
from logging import Logger
from typing import TYPE_CHECKING, Any

import asyncpg
import attrs
from anyio import (
    EndOfStream,
    create_memory_object_stream,
    move_on_after,
)
from anyio.abc import TaskStatus
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from asyncpg import Connection, InterfaceError
from attr.validators import instance_of

from .._events import Event
from .._exceptions import SerializationError
from .._utils import create_repr
from .base import BaseExternalEventBroker

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine


@attrs.define(eq=False, repr=False)
class AsyncpgEventBroker(BaseExternalEventBroker):
    """
    An asynchronous, asyncpg_ based event broker that uses a PostgreSQL server to
    broadcast events using its ``NOTIFY`` mechanism.

    .. _asyncpg: https://pypi.org/project/asyncpg/

    :param dsn: a libpq connection string (e.g.
        ``postgres://user:pass@host:port/dbname``)
    :param options: extra keyword arguments passed to :func:`asyncpg.connection.connect`
    :param channel: the ``NOTIFY`` channel to use
    :param max_idle_time: maximum time to let the connection go idle, before sending a
        ``SELECT 1`` query to prevent a connection timeout
    """

    dsn: str
    options: Mapping[str, Any] = attrs.field(
        factory=dict, validator=instance_of(Mapping)
    )
    channel: str = attrs.field(kw_only=True, default="apscheduler")
    max_idle_time: float = attrs.field(kw_only=True, default=10)

    _send: MemoryObjectSendStream[str] = attrs.field(init=False)

    @classmethod
    def from_async_sqla_engine(
        cls,
        engine: AsyncEngine,
        options: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> AsyncpgEventBroker:
        """
        Create a new asyncpg event broker from an SQLAlchemy engine.

        The engine will only be used to create the appropriate options for
        :func:`asyncpg.connection.connect`.

        :param engine: an asynchronous SQLAlchemy engine using asyncpg as the driver
        :type engine: ~sqlalchemy.ext.asyncio.AsyncEngine
        :param options: extra keyword arguments passed to
            :func:`asyncpg.connection.connect`
        :param kwargs: keyword arguments to pass to the initializer of this class
        :return: the newly created event broker

        """
        if engine.dialect.driver != "asyncpg":
            raise ValueError(
                f'The driver in the engine must be "asyncpg" (current: '
                f"{engine.dialect.driver})"
            )

        dsn = engine.url.render_as_string(hide_password=False).replace("+asyncpg", "")
        return cls(dsn, options or {}, **kwargs)

    def __repr__(self) -> str:
        return create_repr(self, "dsn")

    @property
    def _temporary_failure_exceptions(self) -> tuple[type[Exception], ...]:
        return OSError, InterfaceError

    @asynccontextmanager
    async def _connect(self) -> AsyncGenerator[asyncpg.Connection, None]:
        async for attempt in self._retry():
            with attempt:
                conn = await asyncpg.connect(self.dsn, **self.options)
                try:
                    yield conn
                finally:
                    with move_on_after(5, shield=True):
                        await conn.close(timeout=3)

    async def start(self, exit_stack: AsyncExitStack, logger: Logger) -> None:
        await super().start(exit_stack, logger)
        self._send, receive = create_memory_object_stream[str](100)
        await exit_stack.enter_async_context(self._send)
        await self._task_group.start(self._listen_notifications, receive)

    async def _listen_notifications(
        self, receive: MemoryObjectReceiveStream[str], *, task_status: TaskStatus[None]
    ) -> None:
        conn: Connection

        def listen_callback(
            connection: Connection, pid: int, channel: str, payload: str
        ) -> None:
            event = self.reconstitute_event_str(payload)
            if event is not None:
                self._task_group.start_soon(self.publish_local, event)

        async def unsubscribe() -> None:
            if not conn.is_closed():
                with move_on_after(3, shield=True):
                    await conn.remove_listener(self.channel, listen_callback)

        task_started_sent = False
        with receive:
            while True:
                async with AsyncExitStack() as exit_stack:
                    conn = await exit_stack.enter_async_context(self._connect())
                    self._logger.info("Connection established")
                    try:
                        await conn.add_listener(self.channel, listen_callback)
                        exit_stack.push_async_callback(unsubscribe)
                        if not task_started_sent:
                            task_status.started()
                            task_started_sent = True

                        while True:
                            notification: str | None = None
                            with move_on_after(self.max_idle_time):
                                try:
                                    notification = await receive.receive()
                                except EndOfStream:
                                    self._logger.info("Stream finished")
                                    return

                            if notification:
                                await conn.execute(
                                    "SELECT pg_notify($1, $2)",
                                    self.channel,
                                    notification,
                                )
                            else:
                                await conn.execute("SELECT 1")
                    except InterfaceError as exc:
                        self._logger.error("Connection error: %s", exc)

    async def publish(self, event: Event) -> None:
        notification = self.generate_notification_str(event)
        if len(notification) > 7999:
            raise SerializationError(
                "Serialized event object exceeds 7999 bytes in size"
            )

        await self._send.send(notification)
