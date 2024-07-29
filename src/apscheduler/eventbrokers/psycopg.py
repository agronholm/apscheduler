from __future__ import annotations

from collections.abc import AsyncGenerator, Mapping
from contextlib import AsyncExitStack, asynccontextmanager
from logging import Logger
from typing import TYPE_CHECKING, Any

import attrs
from anyio import (
    EndOfStream,
    create_memory_object_stream,
    move_on_after,
)
from anyio.abc import TaskStatus
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from attr.validators import instance_of
from psycopg import AsyncConnection, InterfaceError

from .._events import Event
from .._exceptions import SerializationError
from .._utils import create_repr
from .._validators import positive_number
from .base import BaseExternalEventBroker

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine


def convert_options(value: Mapping[str, Any]) -> dict[str, Any]:
    return dict(value, autocommit=True)


@attrs.define(eq=False, repr=False)
class PsycopgEventBroker(BaseExternalEventBroker):
    """
    An asynchronous, psycopg_ based event broker that uses a PostgreSQL server to
    broadcast events using its ``NOTIFY`` mechanism.

    .. _psycopg: https://pypi.org/project/psycopg/

    :param conninfo: a libpq connection string (e.g.
        ``postgres://user:pass@host:port/dbname``)
    :param options: extra keyword arguments passed to
        :meth:`psycopg.AsyncConnection.connect`
    :param channel: the ``NOTIFY`` channel to use
    :param max_idle_time: maximum time (in seconds) to let the connection go idle,
        before sending a ``SELECT 1`` query to prevent a connection timeout
    """

    conninfo: str = attrs.field(validator=instance_of(str))
    options: Mapping[str, Any] = attrs.field(
        factory=dict, converter=convert_options, validator=instance_of(Mapping)
    )
    channel: str = attrs.field(
        kw_only=True, default="apscheduler", validator=instance_of(str)
    )
    max_idle_time: float = attrs.field(
        kw_only=True, default=10, validator=[instance_of((int, float)), positive_number]
    )

    _send: MemoryObjectSendStream[str] = attrs.field(init=False)

    @classmethod
    def from_async_sqla_engine(
        cls,
        engine: AsyncEngine,
        options: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> PsycopgEventBroker:
        """
        Create a new psycopg event broker from a SQLAlchemy engine.

        The engine will only be used to create the appropriate options for
        :meth:`psycopg.AsyncConnection.connect`.

        :param engine: an asynchronous SQLAlchemy engine using psycopg as the driver
        :type engine: ~sqlalchemy.ext.asyncio.AsyncEngine
        :param options: extra keyword arguments passed to
            :meth:`psycopg.AsyncConnection.connect`
        :param kwargs: keyword arguments to pass to the initializer of this class
        :return: the newly created event broker

        """
        if engine.dialect.driver != "psycopg":
            raise ValueError(
                f'The driver in the engine must be "psycopg" (current: '
                f"{engine.dialect.driver})"
            )

        conninfo = engine.url.render_as_string(hide_password=False).replace(
            "+psycopg", ""
        )
        return cls(conninfo, options or {}, **kwargs)

    def __repr__(self) -> str:
        return create_repr(self, "conninfo")

    @property
    def _temporary_failure_exceptions(self) -> tuple[type[Exception], ...]:
        return OSError, InterfaceError

    @asynccontextmanager
    async def _connect(self) -> AsyncGenerator[AsyncConnection, None]:
        async for attempt in self._retry():
            with attempt:
                conn = await AsyncConnection.connect(self.conninfo, **self.options)
                try:
                    yield conn
                finally:
                    with move_on_after(5, shield=True):
                        await conn.close()

    async def start(self, exit_stack: AsyncExitStack, logger: Logger) -> None:
        await super().start(exit_stack, logger)
        self._send, receive = create_memory_object_stream[str](100)
        try:
            await exit_stack.enter_async_context(self._send)
            await self._task_group.start(self._listen_notifications)
            exit_stack.callback(self._task_group.cancel_scope.cancel)
            await self._task_group.start(self._publish_notifications, receive)
        except BaseException:
            receive.close()
            raise

    async def _listen_notifications(self, *, task_status: TaskStatus[None]) -> None:
        task_started_sent = False
        while True:
            async with self._connect() as conn:
                try:
                    await conn.execute(f"LISTEN {self.channel}")

                    if not task_started_sent:
                        task_status.started()
                        task_started_sent = True

                    self._logger.debug("Listen connection established")
                    async for notify in conn.notifies():
                        if event := self.reconstitute_event_str(notify.payload):
                            await self.publish_local(event)
                except InterfaceError as exc:
                    self._logger.error("Connection error: %s", exc)

    async def _publish_notifications(
        self, receive: MemoryObjectReceiveStream[str], *, task_status: TaskStatus[None]
    ) -> None:
        task_started_sent = False
        with receive:
            while True:
                async with self._connect() as conn:
                    if not task_started_sent:
                        task_status.started()
                        task_started_sent = True

                    self._logger.debug("Publish connection established")
                    notification: str | None = None
                    while True:
                        with move_on_after(self.max_idle_time):
                            try:
                                notification = await receive.receive()
                            except EndOfStream:
                                return

                        if notification:
                            await conn.execute(
                                "SELECT pg_notify(%t, %t)", [self.channel, notification]
                            )
                        else:
                            await conn.execute("SELECT 1")

    async def publish(self, event: Event) -> None:
        notification = self.generate_notification_str(event)
        if len(notification) > 7999:
            raise SerializationError(
                "Serialized event object exceeds 7999 bytes in size"
            )

        await self._send.send(notification)
