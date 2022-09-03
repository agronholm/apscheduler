from __future__ import annotations

from threading import Thread

import attrs
import tenacity
from redis import ConnectionError, ConnectionPool, Redis, RedisCluster
from redis.client import PubSub

from .. import RetrySettings
from .._events import Event
from ..abc import Serializer
from ..serializers.json import JSONSerializer
from .base import DistributedEventBrokerMixin
from .local import LocalEventBroker


@attrs.define(eq=False)
class RedisEventBroker(LocalEventBroker, DistributedEventBrokerMixin):
    """
    An event broker that uses a Redis server to broadcast events.

    Requires the redis_ library to be installed.

    .. _redis: https://pypi.org/project/redis/

    :param client: a (synchronous) Redis client
    :param serializer: the serializer used to (de)serialize events for transport
    :param channel: channel on which to send the messages
    :param stop_check_interval: interval (in seconds) on which the channel listener
        should check if it should stop (higher values mean slower reaction time but less
        CPU use)
    """

    client: Redis | RedisCluster
    serializer: Serializer = attrs.field(factory=JSONSerializer)
    channel: str = attrs.field(kw_only=True, default="apscheduler")
    stop_check_interval: float = attrs.field(kw_only=True, default=1)
    retry_settings: RetrySettings = attrs.field(default=RetrySettings())
    _stopped: bool = attrs.field(init=False, default=True)
    _thread: Thread = attrs.field(init=False)

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

    def _retry(self) -> tenacity.Retrying:
        def after_attempt(retry_state: tenacity.RetryCallState) -> None:
            self._logger.warning(
                f"{self.__class__.__name__}: connection failure "
                f"(attempt {retry_state.attempt_number}): "
                f"{retry_state.outcome.exception()}",
            )

        return tenacity.Retrying(
            stop=self.retry_settings.stop,
            wait=self.retry_settings.wait,
            retry=tenacity.retry_if_exception_type(ConnectionError),
            after=after_attempt,
            reraise=True,
        )

    def start(self) -> None:
        pubsub = self.client.pubsub()
        pubsub.subscribe(self.channel)
        self._stopped = False
        super().start()
        self._thread = Thread(
            target=self._listen_messages,
            args=[pubsub],
            daemon=True,
            name="Redis subscriber",
        )
        self._thread.start()

    def stop(self, *, force: bool = False) -> None:
        self._stopped = True
        if not force:
            self._thread.join(5)

        super().stop(force=force)

    def _listen_messages(self, pubsub: PubSub) -> None:
        while not self._stopped:
            try:
                for attempt in self._retry():
                    with attempt:
                        msg = pubsub.get_message(
                            ignore_subscribe_messages=True,
                            timeout=self.stop_check_interval,
                        )

                if msg and isinstance(msg["data"], bytes):
                    event = self.reconstitute_event(msg["data"])
                    if event is not None:
                        self.publish_local(event)
            except Exception:
                self._logger.exception(f"{self.__class__.__name__} listener crashed")
                pubsub.close()
                raise

        pubsub.close()

    def publish(self, event: Event) -> None:
        notification = self.generate_notification(event)
        for attempt in self._retry():
            with attempt:
                self.client.publish(self.channel, notification)
