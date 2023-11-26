from __future__ import annotations

from collections.abc import Awaitable, Mapping
from contextlib import AsyncExitStack
from functools import partial
from logging import Logger
from typing import TYPE_CHECKING, Any, Callable, cast

import asyncpg
import attrs
from anyio import (
    TASK_STATUS_IGNORED,
    EndOfStream,
    create_memory_object_stream,
    move_on_after,
)
from anyio.streams.memory import MemoryObjectSendStream
from asyncpg import Connection, InterfaceError

from .._events import Event
from .._exceptions import SerializationError
from .base import BaseExternalEventBroker

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine


@attrs.define(eq=False)
class AsyncpgEventBroker(BaseExternalEventBroker):
    """
    An asynchronous, asyncpg_ based event broker that uses a PostgreSQL server to
    broadcast events using its ``NOTIFY`` mechanism.

    .. _asyncpg: https://pypi.org/project/asyncpg/

    :param connection_factory: a callable that creates an asyncpg connection
    :param channel: the ``NOTIFY`` channel to use
    :param max_idle_time: maximum time to let the connection go idle, before sending a
        ``SELECT 1`` query to prevent a connection timeout
    """

    connection_factory: Callable[[], Awaitable[Connection]]
    channel: str = attrs.field(kw_only=True, default="apscheduler")
    max_idle_time: float = attrs.field(kw_only=True, default=10)
    _send: MemoryObjectSendStream[str] = attrs.field(init=False)

    @classmethod
    def from_dsn(
        cls, dsn: str, options: Mapping[str, Any] | None = None, **kwargs: Any
    ) -> AsyncpgEventBroker:
        """
        Create a new asyncpg event broker from an existing asyncpg connection pool.

        :param dsn: data source name, passed as first positional argument to
            :func:`asyncpg.connect`
        :param options: keyword arguments passed to :func:`asyncpg.connect`
        :param kwargs: keyword arguments to pass to the initializer of this class
        :return: the newly created event broker

        """
        factory = partial(asyncpg.connect, dsn, **(options or {}))
        return cls(factory, **kwargs)

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
        :func:`asyncpg.connect`.

        :param engine: an asynchronous SQLAlchemy engine using asyncpg as the driver
        :type engine: ~sqlalchemy.ext.asyncio.AsyncEngine
        :param options: extra keyword arguments passed to :func:`asyncpg.connect` (will
            override any automatically generated arguments based on the engine)
        :param kwargs: keyword arguments to pass to the initializer of this class
        :return: the newly created event broker

        """
        if engine.dialect.driver != "asyncpg":
            raise ValueError(
                f'The driver in the engine must be "asyncpg" (current: '
                f"{engine.dialect.driver})"
            )

        connect_args = dict(engine.url.query)
        for optname in ("host", "port", "database", "username", "password"):
            value = getattr(engine.url, optname)
            if value is not None:
                if optname == "username":
                    optname = "user"

                connect_args[optname] = value

        if options:
            connect_args |= options

        factory = partial(asyncpg.connect, **connect_args)
        return cls(factory, **kwargs)

    @property
    def _temporary_failure_exceptions(self) -> tuple[type[Exception], ...]:
        return OSError, InterfaceError

    async def start(self, exit_stack: AsyncExitStack, logger: Logger) -> None:
        await super().start(exit_stack, logger)
        self._send = cast(
            MemoryObjectSendStream[str],
            await self._task_group.start(self._listen_notifications),
        )
        await exit_stack.enter_async_context(self._send)

    async def _listen_notifications(self, *, task_status=TASK_STATUS_IGNORED) -> None:
        conn: Connection

        def listen_callback(
            connection: Connection, pid: int, channel: str, payload: str
        ) -> None:
            event = self.reconstitute_event_str(payload)
            if event is not None:
                self._task_group.start_soon(self.publish_local, event)

        async def close_connection() -> None:
            if not conn.is_closed():
                with move_on_after(3, shield=True):
                    await conn.close()

        async def unsubscribe() -> None:
            if not conn.is_closed():
                with move_on_after(3, shield=True):
                    await conn.remove_listener(self.channel, listen_callback)

        task_started_sent = False
        send, receive = create_memory_object_stream[str](100)
        while True:
            async with AsyncExitStack() as exit_stack:
                async for attempt in self._retry():
                    with attempt:
                        conn = await self.connection_factory()

                exit_stack.push_async_callback(close_connection)
                self._logger.info("Connection established")
                try:
                    await conn.add_listener(self.channel, listen_callback)
                    exit_stack.push_async_callback(unsubscribe)
                    if not task_started_sent:
                        task_status.started(send)
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
                                "SELECT pg_notify($1, $2)", self.channel, notification
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
