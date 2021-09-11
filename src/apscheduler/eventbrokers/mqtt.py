from __future__ import annotations

from concurrent.futures import Future
from logging import Logger, getLogger
from typing import Any, Optional

import attr
from paho.mqtt.client import Client, MQTTMessage
from paho.mqtt.properties import Properties
from paho.mqtt.reasoncodes import ReasonCodes

from ..abc import Serializer
from ..events import Event
from ..serializers.json import JSONSerializer
from ..util import reentrant
from .base import DistributedEventBrokerMixin
from .local import LocalEventBroker


@reentrant
@attr.define(eq=False)
class MQTTEventBroker(LocalEventBroker, DistributedEventBrokerMixin):
    client: Client
    serializer: Serializer = attr.field(factory=JSONSerializer)
    host: str = attr.field(kw_only=True, default='localhost')
    port: int = attr.field(kw_only=True, default=1883)
    topic: str = attr.field(kw_only=True, default='apscheduler')
    subscribe_qos: int = attr.field(kw_only=True, default=0)
    publish_qos: int = attr.field(kw_only=True, default=0)
    _logger: Logger = attr.field(init=False, factory=lambda: getLogger(__name__))
    _ready_future: Future[None] = attr.field(init=False)

    def __enter__(self):
        self._ready_future = Future()
        self.client.enable_logger(self._logger)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_subscribe = self._on_subscribe
        self.client.connect(self.host, self.port)
        self.client.loop_start()
        self._ready_future.result(10)
        return super().__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.disconnect()
        self.client.loop_stop(force=exc_type is not None)
        return super().__exit__(exc_type, exc_val, exc_tb)

    def _on_connect(self, client: Client, userdata: Any, flags: dict[str, Any],
                    rc: ReasonCodes | int, properties: Optional[Properties] = None) -> None:
        try:
            client.subscribe(self.topic, qos=self.subscribe_qos)
        except Exception as exc:
            self._ready_future.set_exception(exc)
            raise

    def _on_subscribe(self, client: Client, userdata: Any, mid, granted_qos: list[int]) -> None:
        self._ready_future.set_result(None)

    def _on_message(self, client: Client, userdata: Any, msg: MQTTMessage) -> None:
        event = self.reconstitute_event(msg.payload)
        if event is not None:
            super().publish(event)

    def publish(self, event: Event) -> None:
        notification = self.generate_notification(event)
        self.client.publish(self.topic, notification, qos=self.publish_qos)
