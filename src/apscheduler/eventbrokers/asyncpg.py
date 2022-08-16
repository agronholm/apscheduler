from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, AsyncContextManager, AsyncGenerator, Callable

import attrs
from anyio import TASK_STATUS_IGNORED, CancelScope, sleep
from asyncpg import Connection
from asyncpg.pool import Pool

from .._events import Event
from .._exceptions import SerializationError
from ..abc import Serializer
from ..serializers.json import JSONSerializer
from .async_local import LocalAsyncEventBroker
from .base import DistributedEventBrokerMixin

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine


@attrs.define(eq=False)
class AsyncpgEventBroker(LocalAsyncEventBroker, DistributedEventBrokerMixin):
    """
    An asynchronous, asyncpg_ based event broker that uses a PostgreSQL server to
    broadcast events using its ``NOTIFY`` mechanism.

    .. _asyncpg: https://pypi.org/project/asyncpg/

    :param connection_factory: a callable that creates an async context manager that
        yields a new asyncpg connection
    :param serializer: the serializer used to (de)serialize events for transport
    :param channel: the ``NOTIFY`` channel to use
    :param max_idle_time: maximum time to let the connection go idle, before sending a
        ``SELECT 1`` query to prevent a connection timeout
    """

    connection_factory: Callable[[], AsyncContextManager[Connection]]
    serializer: Serializer = attrs.field(kw_only=True, factory=JSONSerializer)
    channel: str = attrs.field(kw_only=True, default="apscheduler")
    max_idle_time: float = attrs.field(kw_only=True, default=30)
    _listen_cancel_scope: CancelScope = attrs.field(init=False)

    @classmethod
    def from_asyncpg_pool(cls, pool: Pool, **kwargs) -> AsyncpgEventBroker:
        """
        Create a new asyncpg event broker from an existing asyncpg connection pool.

        :param pool: an asyncpg connection pool
        :param kwargs: keyword arguments to pass to the initializer of this class
        :return: the newly created event broker

        """
        return cls(pool.acquire, **kwargs)

    @classmethod
    def from_async_sqla_engine(
        cls, engine: AsyncEngine, **kwargs
    ) -> AsyncpgEventBroker:
        """
        Create a new asyncpg event broker from an SQLAlchemy engine.

        :param engine: an asynchronous SQLAlchemy engine using asyncpg as the driver
        :type engine: ~sqlalchemy.ext.asyncio.AsyncEngine
        :param kwargs: keyword arguments to pass to the initializer of this class
        :return: the newly created event broker

        """
        if engine.dialect.driver != "asyncpg":
            raise ValueError(
                f'The driver in the engine must be "asyncpg" (current: '
                f"{engine.dialect.driver})"
            )

        @asynccontextmanager
        async def connection_factory() -> AsyncGenerator[Connection, None]:
            conn = await engine.raw_connection()
            try:
                yield conn.connection._connection
            finally:
                conn.close()

        return cls(connection_factory, **kwargs)

    async def start(self) -> None:
        await super().start()
        self._listen_cancel_scope = await self._task_group.start(
            self._listen_notifications
        )

    async def stop(self, *, force: bool = False) -> None:
        self._listen_cancel_scope.cancel()
        await super().stop(force=force)

    async def _listen_notifications(self, *, task_status=TASK_STATUS_IGNORED) -> None:
        def callback(connection, pid, channel: str, payload: str) -> None:
            event = self.reconstitute_event_str(payload)
            if event is not None:
                self._task_group.start_soon(self.publish_local, event)

        task_started_sent = False
        with CancelScope() as cancel_scope:
            while True:
                async with self.connection_factory() as conn:
                    await conn.add_listener(self.channel, callback)
                    if not task_started_sent:
                        task_status.started(cancel_scope)
                        task_started_sent = True

                    try:
                        while True:
                            await sleep(self.max_idle_time)
                            await conn.execute("SELECT 1")
                    finally:
                        await conn.remove_listener(self.channel, callback)

    async def publish(self, event: Event) -> None:
        notification = self.generate_notification_str(event)
        if len(notification) > 7999:
            raise SerializationError(
                "Serialized event object exceeds 7999 bytes in size"
            )

        async with self.connection_factory() as conn:
            await conn.execute("SELECT pg_notify($1, $2)", self.channel, notification)
            return
