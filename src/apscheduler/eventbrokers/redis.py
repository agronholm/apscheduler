from __future__ import annotations

from asyncio import CancelledError
from contextlib import AsyncExitStack
from logging import Logger

import anyio
import attrs
import tenacity
from redis import ConnectionError
from redis.asyncio import Redis
from redis.asyncio.client import PubSub
from redis.asyncio.connection import ConnectionPool

from .._events import Event
from .base import BaseExternalEventBroker


@attrs.define(eq=False)
class RedisEventBroker(BaseExternalEventBroker):
    """
    An event broker that uses a Redis server to broadcast events.

    Requires the redis_ library to be installed.

    .. _redis: https://pypi.org/project/redis/

    :param client: an asynchronous Redis client
    :param channel: channel on which to send the messages
    :param stop_check_interval: interval (in seconds) on which the channel listener
        should check if it should stop (higher values mean slower reaction time but less
        CPU use)
    """

    client: Redis
    channel: str = attrs.field(kw_only=True, default="apscheduler")
    stop_check_interval: float = attrs.field(kw_only=True, default=1)
    _stopped: bool = attrs.field(init=False, default=True)

    @classmethod
    def from_url(cls, url: str, **kwargs) -> RedisEventBroker:
        """
        Create a new event broker from a URL.

        :param url: a Redis URL (```redis://...```)
        :param kwargs: keyword arguments to pass to the initializer of this class
        :return: the newly created event broker

        """
        pool = ConnectionPool.from_url(url)
        client = Redis(connection_pool=pool)
        return cls(client, **kwargs)

    def _retry(self) -> tenacity.AsyncRetrying:
        def after_attempt(retry_state: tenacity.RetryCallState) -> None:
            self._logger.warning(
                "%s: connection failure (attempt %d): %s",
                self.__class__.__name__,
                retry_state.attempt_number,
                retry_state.outcome.exception(),
            )

        return tenacity.AsyncRetrying(
            stop=self.retry_settings.stop,
            wait=self.retry_settings.wait,
            retry=tenacity.retry_if_exception_type(ConnectionError),
            after=after_attempt,
            sleep=anyio.sleep,
            reraise=True,
        )

    async def start(self, exit_stack: AsyncExitStack, logger: Logger) -> None:
        await super().start(exit_stack, logger)
        pubsub = self.client.pubsub()
        await pubsub.subscribe(self.channel)

        self._stopped = False
        exit_stack.callback(setattr, self, "_stopped", True)
        self._task_group.start_soon(
            self._listen_messages, pubsub, name="Redis subscriber"
        )

    async def _listen_messages(self, pubsub: PubSub) -> None:
        while not self._stopped:
            try:
                async for attempt in self._retry():
                    with attempt:
                        msg = await pubsub.get_message(
                            ignore_subscribe_messages=True,
                            timeout=self.stop_check_interval,
                        )

                if msg and isinstance(msg["data"], bytes):
                    event = self.reconstitute_event(msg["data"])
                    if event is not None:
                        await self.publish_local(event)
            except Exception as exc:
                # CancelledError is a subclass of Exception in Python 3.7
                if not isinstance(exc, CancelledError):
                    self._logger.exception(
                        "%s listener crashed", self.__class__.__name__
                    )

                await pubsub.close()
                raise

    async def publish(self, event: Event) -> None:
        notification = self.generate_notification(event)
        async for attempt in self._retry():
            with attempt:
                await self.client.publish(self.channel, notification)
