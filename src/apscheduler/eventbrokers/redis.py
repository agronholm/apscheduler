from __future__ import annotations

from concurrent.futures import Future
from threading import Thread

import attrs
from redis import ConnectionPool, Redis

from ..abc import Serializer
from ..events import Event
from ..serializers.json import JSONSerializer
from ..util import reentrant
from .base import DistributedEventBrokerMixin
from .local import LocalEventBroker


@reentrant
@attrs.define(eq=False)
class RedisEventBroker(LocalEventBroker, DistributedEventBrokerMixin):
    client: Redis
    serializer: Serializer = attrs.field(factory=JSONSerializer)
    channel: str = attrs.field(kw_only=True, default='apscheduler')
    message_poll_interval: float = attrs.field(kw_only=True, default=0.05)
    _stopped: bool = attrs.field(init=False, default=True)
    _ready_future: Future[None] = attrs.field(init=False)

    @classmethod
    def from_url(cls, url: str, **kwargs) -> RedisEventBroker:
        pool = ConnectionPool.from_url(url, **kwargs)
        client = Redis(connection_pool=pool)
        return cls(client)

    def __enter__(self):
        self._stopped = False
        self._ready_future = Future()
        self._thread = Thread(target=self._listen_messages, daemon=True, name='Redis subscriber')
        self._thread.start()
        self._ready_future.result(10)
        return super().__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._stopped = True
        if not exc_type:
            self._thread.join(5)

        super().__exit__(exc_type, exc_val, exc_tb)

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
                    if msg and isinstance(msg['data'], bytes):
                        event = self.reconstitute_event(msg['data'])
                        if event is not None:
                            self.publish_local(event)
            except BaseException:
                self._logger.exception('Subscriber crashed')
                raise
            finally:
                pubsub.close()

    def publish(self, event: Event) -> None:
        notification = self.generate_notification(event)
        self.client.publish(self.channel, notification)
