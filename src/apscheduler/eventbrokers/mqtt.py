from __future__ import annotations

import sys
from concurrent.futures import Future
from contextlib import AsyncExitStack
from logging import Logger
from ssl import SSLContext
from typing import Any

import attrs
from anyio import to_thread
from anyio.from_thread import BlockingPortal
from attr.validators import in_, instance_of, optional
from paho.mqtt.client import Client, MQTTMessage
from paho.mqtt.enums import CallbackAPIVersion

from .._events import Event
from .._utils import create_repr
from .base import BaseExternalEventBroker

ALLOWED_TRANSPORTS = ("mqtt", "mqtts", "ws", "wss", "unix")


@attrs.define(eq=False, repr=False)
class MQTTEventBroker(BaseExternalEventBroker):
    """
    An event broker that uses an MQTT (v3.1 or v5) broker to broadcast events.

    Requires the paho-mqtt_ library (v2.0 or later) to be installed.

    .. _paho-mqtt: https://pypi.org/project/paho-mqtt/

    :param host: MQTT broker host (or UNIX socket path)
    :param port: MQTT broker port (for ``tcp`` or ``websocket`` transports)
    :param transport: one of ``tcp``, ``websocket`` or ``unix`` (default: ``tcp``)
    :param client_id: MQTT client ID (needed to resume an MQTT session if a connection
        is broken)
    :param ssl: either ``True`` or a custom SSL context to enable SSL/TLS, ``False`` to
        disable
    :param topic: topic on which to send the messages
    :param subscribe_qos: MQTT QoS to use for subscribing messages
    :param publish_qos: MQTT QoS to use for publishing messages
    """

    host: str = attrs.field(default="localhost", validator=instance_of(str))
    port: int | None = attrs.field(default=None, validator=optional(instance_of(int)))
    transport: str = attrs.field(
        default="tcp", validator=in_(["tcp", "websocket", "unix"])
    )
    client_id: str | None = attrs.field(
        default=None, validator=optional(instance_of(str))
    )
    ssl: bool | SSLContext = attrs.field(
        default=False, validator=instance_of((bool, SSLContext))
    )
    topic: str = attrs.field(
        kw_only=True, default="apscheduler", validator=instance_of(str)
    )
    subscribe_qos: int = attrs.field(kw_only=True, default=0, validator=in_([0, 1, 2]))
    publish_qos: int = attrs.field(kw_only=True, default=0, validator=in_([0, 1, 2]))

    _use_tls: bool = attrs.field(init=False, default=False)
    _client: Client = attrs.field(init=False)
    _portal: BlockingPortal = attrs.field(init=False)
    _ready_future: Future[None] = attrs.field(init=False)

    def __attrs_post_init__(self) -> None:
        if self.port is None:
            if self.transport == "tcp":
                self.port = 8883 if self.ssl else 1883
            elif self.transport == "websocket":
                self.port = 443 if self.ssl else 80

        self._client = Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id=self.client_id,
            transport=self.transport,
        )
        if isinstance(self.ssl, SSLContext):
            self._client.tls_set_context(self.ssl)
        elif self.ssl:
            self._client.tls_set()

    def __repr__(self) -> str:
        return create_repr(self, "host", "port", "transport")

    async def start(self, exit_stack: AsyncExitStack, logger: Logger) -> None:
        await super().start(exit_stack, logger)
        self._portal = await exit_stack.enter_async_context(BlockingPortal())
        self._ready_future = Future()
        self._client.on_connect = self._on_connect
        self._client.on_connect_fail = self._on_connect_fail
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message
        self._client.on_subscribe = self._on_subscribe
        self._client.connect_async(self.host, self.port)
        self._client.loop_start()
        exit_stack.push_async_callback(to_thread.run_sync, self._client.loop_stop)

        # Wait for the connection attempt to be done
        await to_thread.run_sync(self._ready_future.result, 10)

        # Schedule a disconnection for when the exit stack is exited
        exit_stack.callback(self._client.disconnect)

    def _on_connect(self, client: Client, *_: Any) -> None:
        self._logger.info("%s: Connected", self.__class__.__name__)
        try:
            client.subscribe(self.topic, qos=self.subscribe_qos)
        except Exception as exc:
            self._ready_future.set_exception(exc)
            raise

    def _on_connect_fail(self, *_: Any) -> None:
        exc = sys.exc_info()[1]
        self._logger.error("%s: Connection failed (%s)", self.__class__.__name__, exc)

    def _on_disconnect(self, *args: Any) -> None:
        reason_code = args[3] if len(args) == 5 else args[2]
        self._logger.error(
            "%s: Disconnected (code: %s)", self.__class__.__name__, reason_code
        )

    def _on_subscribe(self, *_: Any) -> None:
        self._logger.info("%s: Subscribed", self.__class__.__name__)
        self._ready_future.set_result(None)

    def _on_message(self, _: Any, __: Any, msg: MQTTMessage) -> None:
        event = self.reconstitute_event(msg.payload)
        if event is not None:
            self._portal.call(self.publish_local, event)

    async def publish(self, event: Event) -> None:
        notification = self.generate_notification(event)
        await to_thread.run_sync(
            lambda: self._client.publish(self.topic, notification, qos=self.publish_qos)
        )
