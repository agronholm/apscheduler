from __future__ import annotations

from contextlib import asynccontextmanager
from logging import Logger, getLogger
from typing import TYPE_CHECKING, AsyncContextManager, AsyncGenerator, Callable

import attr
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
@attr.define(eq=False)
class AsyncpgEventBroker(LocalAsyncEventBroker, DistributedEventBrokerMixin):
    connection_factory: Callable[[], AsyncContextManager[Connection]]
    channel: str = attr.field(kw_only=True, default='apscheduler')
    max_idle_time: float = attr.field(kw_only=True, default=30)
    _logger: Logger = attr.field(init=False, factory=lambda: getLogger(__name__))

    @classmethod
    def from_asyncpg_pool(cls, pool: Pool) -> AsyncpgEventBroker:
        return cls(pool.acquire)

    @classmethod
    def from_async_sqla_engine(cls, engine: AsyncEngine) -> AsyncpgEventBroker:
        if engine.dialect.driver != 'asyncpg':
            raise ValueError(f'The driver in the engine must be "asyncpg" (current: '
                             f'{engine.dialect.driver})')

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
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._task_group.cancel_scope.cancel()
        await super().__aexit__(exc_type, exc_val, exc_tb)

    async def _listen_notifications(self, *, task_status=TASK_STATUS_IGNORED) -> None:
        local_publish = super(AsyncpgEventBroker, self).publish

        def callback(connection, pid, channel: str, payload: str) -> None:
            event = self.reconstitute_event_str(payload)
            if event is not None:
                self._task_group.start_soon(local_publish, event)

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
                        await conn.execute('SELECT 1')
                finally:
                    await conn.remove_listener(self.channel, callback)

    async def publish(self, event: Event) -> None:
        notification = self.generate_notification_str(event)
        if len(notification) > 7999:
            raise SerializationError('Serialized event object exceeds 7999 bytes in size')

        async with self.connection_factory() as conn:
            await conn.execute("SELECT pg_notify($1, $2)", self.channel, notification)
            return
