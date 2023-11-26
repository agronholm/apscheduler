from __future__ import annotations

import sys
from concurrent.futures import Future
from contextlib import AsyncExitStack
from logging import Logger
from typing import Any

import attrs
from anyio import to_thread
from anyio.from_thread import BlockingPortal
from paho.mqtt.client import Client, MQTTMessage
from paho.mqtt.properties import Properties
from paho.mqtt.reasoncodes import ReasonCodes

from .._events import Event
from .base import BaseExternalEventBroker


@attrs.define(eq=False)
class MQTTEventBroker(BaseExternalEventBroker):
    """
    An event broker that uses an MQTT (v3.1 or v5) broker to broadcast events.

    Requires the paho-mqtt_ library to be installed.

    .. _paho-mqtt: https://pypi.org/project/paho-mqtt/

    :param client: a paho-mqtt client
    :param host: host name or IP address to connect to
    :param port: TCP port number to connect to
    :param topic: topic on which to send the messages
    :param subscribe_qos: MQTT QoS to use for subscribing messages
    :param publish_qos: MQTT QoS to use for publishing messages
    """

    client: Client = attrs.field(factory=Client)
    host: str = attrs.field(kw_only=True, default="localhost")
    port: int = attrs.field(kw_only=True, default=1883)
    topic: str = attrs.field(kw_only=True, default="apscheduler")
    subscribe_qos: int = attrs.field(kw_only=True, default=0)
    publish_qos: int = attrs.field(kw_only=True, default=0)
    _portal: BlockingPortal = attrs.field(init=False)
    _ready_future: Future[None] = attrs.field(init=False)

    async def start(self, exit_stack: AsyncExitStack, logger: Logger) -> None:
        await super().start(exit_stack, logger)
        self._portal = await exit_stack.enter_async_context(BlockingPortal())
        self._ready_future = Future()
        self.client.on_connect = self._on_connect
        self.client.on_connect_fail = self._on_connect_fail
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        self.client.on_subscribe = self._on_subscribe
        self.client.connect(self.host, self.port)
        self.client.loop_start()
        exit_stack.push_async_callback(to_thread.run_sync, self.client.loop_stop)
        await to_thread.run_sync(self._ready_future.result, 10)
        exit_stack.push_async_callback(to_thread.run_sync, self.client.disconnect)

    def _on_connect(
        self,
        client: Client,
        userdata: Any,
        flags: dict[str, Any],
        rc: ReasonCodes | int,
        properties: Properties | None = None,
    ) -> None:
        self._logger.info("%s: Connected", self.__class__.__name__)
        try:
            client.subscribe(self.topic, qos=self.subscribe_qos)
        except Exception as exc:
            self._ready_future.set_exception(exc)
            raise

    def _on_connect_fail(self, client: Client, userdata: Any) -> None:
        exc = sys.exc_info()[1]
        self._logger.error("%s: Connection failed (%s)", self.__class__.__name__, exc)

    def _on_disconnect(
        self,
        client: Client,
        userdata: Any,
        rc: ReasonCodes | int,
        properties: Properties | None = None,
    ) -> None:
        self._logger.error("%s: Disconnected (code: %s)", self.__class__.__name__, rc)

    def _on_subscribe(
        self, client: Client, userdata: Any, mid: int, granted_qos: list[int]
    ) -> None:
        self._logger.info("%s: Subscribed", self.__class__.__name__)
        self._ready_future.set_result(None)

    def _on_message(self, client: Client, userdata: Any, msg: MQTTMessage) -> None:
        event = self.reconstitute_event(msg.payload)
        if event is not None:
            self._portal.call(self.publish_local, event)

    async def publish(self, event: Event) -> None:
        notification = self.generate_notification(event)
        await to_thread.run_sync(
            lambda: self.client.publish(self.topic, notification, qos=self.publish_qos)
        )
