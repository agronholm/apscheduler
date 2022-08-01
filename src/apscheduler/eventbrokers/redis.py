from __future__ import annotations

from concurrent.futures import Future
from threading import Thread

import attrs
from redis import ConnectionPool, Redis

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
    :param message_poll_interval: interval on which to poll for new messages (higher
        values mean slower reaction time but less CPU use)
    """

    client: Redis
    serializer: Serializer = attrs.field(factory=JSONSerializer)
    channel: str = attrs.field(kw_only=True, default="apscheduler")
    message_poll_interval: float = attrs.field(kw_only=True, default=0.05)
    _stopped: bool = attrs.field(init=False, default=True)
    _ready_future: Future[None] = attrs.field(init=False)
    _thread: Thread = attrs.field(init=False)

    @classmethod
    def from_url(cls, url: str, **kwargs) -> RedisEventBroker:
        """
        Create a new event broker from a URL.

        :param url: a Redis URL (```redis://...```)
        :param kwargs: keyword arguments to pass to the initializer of this class
        :return: the newly created event broker

        """
        pool = ConnectionPool.from_url(url, **kwargs)
        client = Redis(connection_pool=pool)
        return cls(client)

    def start(self) -> None:
        self._stopped = False
        self._ready_future = Future()
        self._thread = Thread(
            target=self._listen_messages, daemon=True, name="Redis subscriber"
        )
        self._thread.start()
        self._ready_future.result(10)
        super().start()

    def stop(self, *, force: bool = False) -> None:
        self._stopped = True
        if not force:
            self._thread.join(5)

        super().stop(force=force)

    def _listen_messages(self) -> None:
        while not self._stopped:
            try:
                pubsub = self.client.pubsub()
                pubsub.subscribe(self.channel)
            except BaseException as exc:
                if not self._ready_future.done():
                    self._ready_future.set_exception(exc)

                raise
            else:
                if not self._ready_future.done():
                    self._ready_future.set_result(None)

            try:
                while not self._stopped:
                    msg = pubsub.get_message(timeout=self.message_poll_interval)
                    if msg and isinstance(msg["data"], bytes):
                        event = self.reconstitute_event(msg["data"])
                        if event is not None:
                            self.publish_local(event)
            except BaseException:
                self._logger.exception("Subscriber crashed")
                raise
            finally:
                pubsub.close()

    def publish(self, event: Event) -> None:
        notification = self.generate_notification(event)
        self.client.publish(self.channel, notification)
