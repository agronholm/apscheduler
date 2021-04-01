from dataclasses import dataclass, field
from typing import Optional

from anyio import create_event, move_on_after
from anyio.abc import Event


@dataclass
class DelayWatcher:
    max_wait_time: Optional[float]
    _event: Optional[Event] = field(init=False, default=None)

    async def wait(self) -> None:
        async with move_on_after(self.max_wait_time):
            self._event = Event()
            await self._event.wait()

    async def notify(self) -> None:
        if self._event:
            event = self._event
            del self._event
            event.set()
