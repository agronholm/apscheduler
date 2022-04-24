from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, AsyncContextManager, AsyncGenerator, Callable

import attrs
from anyio import TASK_STATUS_IGNORED, sleep
from asyncpg import Connection
from asyncpg.pool import Pool

from ..events import Event
from ..exceptions import SerializationError
from ..util import reentrant
from .async_local import LocalAsyncEventBroker
from .base import DistributedEventBrokerMixin

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine


@reentrant
@attrs.define(eq=False)
class AsyncpgEventBroker(LocalAsyncEventBroker, DistributedEventBrokerMixin):
    connection_factory: Callable[[], AsyncContextManager[Connection]]
    channel: str = attrs.field(kw_only=True, default="apscheduler")
    max_idle_time: float = attrs.field(kw_only=True, default=30)

    @classmethod
    def from_asyncpg_pool(cls, pool: Pool) -> AsyncpgEventBroker:
        return cls(pool.acquire)

    @classmethod
    def from_async_sqla_engine(cls, engine: AsyncEngine) -> AsyncpgEventBroker:
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

        return cls(connection_factory)

    async def __aenter__(self) -> LocalAsyncEventBroker:
        await super().__aenter__()
        await self._task_group.start(self._listen_notifications)
        self._exit_stack.callback(self._task_group.cancel_scope.cancel)
        return self

    async def _listen_notifications(self, *, task_status=TASK_STATUS_IGNORED) -> None:
        def callback(connection, pid, channel: str, payload: str) -> None:
            event = self.reconstitute_event_str(payload)
            if event is not None:
                self._task_group.start_soon(self.publish_local, event)

        task_started_sent = False
        while True:
            async with self.connection_factory() as conn:
                await conn.add_listener(self.channel, callback)
                if not task_started_sent:
                    task_status.started()
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
