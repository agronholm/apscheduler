import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, List

from apscheduler.abc import EventHub
from apscheduler.events import Event

logger = logging.getLogger(__name__)


@dataclass
class LocalEventHub(EventHub):
    _callbacks: List[Callable[[Event], Any]] = field(init=False, default_factory=list)

    async def publish(self, event: Event) -> None:
        for callback in self._callbacks:
            try:
                retval = callback(event)
                if isinstance(retval, Coroutine):
                    await retval
            except Exception:
                logger.exception('Error delivering event to callback %r', callback)

    async def subscribe(self, callback: Callable[[Event], Any]) -> None:
        self._callbacks.append(callback)
