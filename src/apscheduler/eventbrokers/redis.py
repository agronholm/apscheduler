from __future__ import annotations

from asyncio import CancelledError
from contextlib import AsyncExitStack
from logging import Logger

import anyio
import attrs
import tenacity
from anyio import move_on_after
from attr.validators import instance_of
from redis import ConnectionError
from redis.asyncio import Redis
from redis.asyncio.client import PubSub
from redis.asyncio.connection import ConnectionPool

from .._events import Event
from .._utils import create_repr
from .base import BaseExternalEventBroker


@attrs.define(eq=False, repr=False)
class RedisEventBroker(BaseExternalEventBroker):
    """
    An event broker that uses a Redis server to broadcast events.

    Requires the redis_ library to be installed.

    .. _redis: https://pypi.org/project/redis/

    :param client_or_url: an asynchronous Redis client or a Redis URL
        (```redis://...```)
    :param channel: channel on which to send the messages
    :param stop_check_interval: interval (in seconds) on which the channel listener
        should check if it should stop (higher values mean slower reaction time but less
        CPU use)

    .. note:: The event broker will not manage the life cycle of any client instance
        passed to it, so you need to close the client afterwards when you're done with
        it.
    """

    client_or_url: Redis | str = attrs.field(validator=instance_of((Redis, str)))
    channel: str = attrs.field(kw_only=True, default="apscheduler")
    stop_check_interval: float = attrs.field(kw_only=True, default=1)

    _client: Redis = attrs.field(init=False)
    _close_on_exit: bool = attrs.field(init=False, default=False)
    _stopped: bool = attrs.field(init=False, default=True)

    def __attrs_post_init__(self) -> None:
        if isinstance(self.client_or_url, str):
            pool = ConnectionPool.from_url(self.client_or_url)
            self._client = Redis(connection_pool=pool)
            self._close_on_exit = True
        else:
            self._client = self.client_or_url

    def __repr__(self) -> str:
        return create_repr(self, "client_or_url")

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

    async def _close_client(self) -> None:
        with move_on_after(5, shield=True):
            await self._client.aclose(close_connection_pool=True)

    async def start(self, exit_stack: AsyncExitStack, logger: Logger) -> None:
        # Close the client and its connection pool if this broker was created using
        # .from_url()
        if self._close_on_exit:
            exit_stack.push_async_callback(self._close_client)

        pubsub = await exit_stack.enter_async_context(self._client.pubsub())
        await pubsub.subscribe(self.channel)
        await super().start(exit_stack, logger)

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

                await pubsub.aclose()
                raise

    async def publish(self, event: Event) -> None:
        notification = self.generate_notification(event)
        async for attempt in self._retry():
            with attempt:
                await self._client.publish(self.channel, notification)
