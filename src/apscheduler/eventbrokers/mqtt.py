from __future__ import annotations

import sys
from concurrent.futures import Future
from typing import Any

import attrs
from paho.mqtt.client import Client, MQTTMessage
from paho.mqtt.properties import Properties
from paho.mqtt.reasoncodes import ReasonCodes

from .._events import Event
from ..abc import Serializer
from ..serializers.json import JSONSerializer
from .base import DistributedEventBrokerMixin
from .local import LocalEventBroker


@attrs.define(eq=False)
class MQTTEventBroker(LocalEventBroker, DistributedEventBrokerMixin):
    """
    An event broker that uses an MQTT (v3.1 or v5) broker to broadcast events.

    Requires the paho-mqtt_ library to be installed.

    .. _paho-mqtt: https://pypi.org/project/paho-mqtt/

    :param client: a paho-mqtt client
    :param serializer: the serializer used to (de)serialize events for transport
    :param host: host name or IP address to connect to
    :param port: TCP port number to connect to
    :param topic: topic on which to send the messages
    :param subscribe_qos: MQTT QoS to use for subscribing messages
    :param publish_qos: MQTT QoS to use for publishing messages
    """

    client: Client = attrs.field(factory=Client)
    serializer: Serializer = attrs.field(factory=JSONSerializer)
    host: str = attrs.field(kw_only=True, default="localhost")
    port: int = attrs.field(kw_only=True, default=1883)
    topic: str = attrs.field(kw_only=True, default="apscheduler")
    subscribe_qos: int = attrs.field(kw_only=True, default=0)
    publish_qos: int = attrs.field(kw_only=True, default=0)
    _ready_future: Future[None] = attrs.field(init=False)

    def start(self) -> None:
        super().start()
        self._ready_future = Future()
        self.client.on_connect = self._on_connect
        self.client.on_connect_fail = self._on_connect_fail
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        self.client.on_subscribe = self._on_subscribe
        self.client.connect(self.host, self.port)
        self.client.loop_start()
        self._ready_future.result(10)

    def stop(self, *, force: bool = False) -> None:
        self.client.disconnect()
        self.client.loop_stop(force=force)
        super().stop()

    def _on_connect(
        self,
        client: Client,
        userdata: Any,
        flags: dict[str, Any],
        rc: ReasonCodes | int,
        properties: Properties | None = None,
    ) -> None:
        self._logger.info(f"{self.__class__.__name__}: Connected")
        try:
            client.subscribe(self.topic, qos=self.subscribe_qos)
        except Exception as exc:
            self._ready_future.set_exception(exc)
            raise

    def _on_connect_fail(self, client: Client, userdata: Any) -> None:
        exc = sys.exc_info()[1]
        self._logger.error(f"{self.__class__.__name__}: Connection failed ({exc})")

    def _on_disconnect(
        self,
        client: Client,
        userdata: Any,
        rc: ReasonCodes | int,
        properties: Properties | None = None,
    ) -> None:
        self._logger.error(f"{self.__class__.__name__}: Disconnected (code: {rc})")

    def _on_subscribe(
        self, client: Client, userdata: Any, mid: int, granted_qos: list[int]
    ) -> None:
        self._logger.info(f"{self.__class__.__name__}: Subscribed")
        self._ready_future.set_result(None)

    def _on_message(self, client: Client, userdata: Any, msg: MQTTMessage) -> None:
        event = self.reconstitute_event(msg.payload)
        if event is not None:
            self.publish_local(event)

    def publish(self, event: Event) -> None:
        notification = self.generate_notification(event)
        self.client.publish(self.topic, notification, qos=self.publish_qos)
