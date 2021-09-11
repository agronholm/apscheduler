from __future__ import annotations

from concurrent.futures import Future
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
    _ready_future: Future[None] = attr.field(init=False)

    def __enter__(self):
        super().__enter__()
        self._ready_future = Future()
        self.client.enable_logger(self._logger)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_subscribe = self._on_subscribe
        self.client.connect(self.host, self.port)
        self.client.loop_start()
        self._ready_future.result(10)
        self._exit_stack.push(lambda exc_type, *_: self.client.loop_stop(force=bool(exc_type)))
        self._exit_stack.callback(self.client.disconnect)
        return self

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
            self.publish_local(event)

    def publish(self, event: Event) -> None:
        notification = self.generate_notification(event)
        self.client.publish(self.topic, notification, qos=self.publish_qos)
