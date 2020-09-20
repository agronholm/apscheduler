from functools import partial
from typing import Any, Callable, Iterable, Mapping, Optional, Union

from anyio import start_blocking_portal
from anyio.abc import BlockingPortal
from apscheduler.abc import Trigger
from apscheduler.events import Event
from apscheduler.schedulers.async_ import AsyncScheduler


class SyncScheduler:
    def __init__(self, *, portal: Optional[BlockingPortal] = None, **kwargs):
        self._scheduler = AsyncScheduler(**kwargs)
        self._portal = portal
        self._shutdown_portal = False

    def __enter__(self):
        if not self._portal:
            self._portal = start_blocking_portal()
            self._shutdown_portal = True

        self._portal.call(self._scheduler.__aenter__)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        cancel_remaining = exc_type is not None
        try:
            self._portal.call(self._scheduler.__aexit__, exc_type, exc_val, exc_tb)
        except BaseException:
            cancel_remaining = True
            raise
        finally:
            if self._shutdown_portal:
                self._portal.stop_from_external_thread(cancel_remaining=cancel_remaining)
                self._portal = None

    def add_schedule(self, task: Union[str, Callable], trigger: Trigger, *,
                     args: Optional[Iterable] = None,
                     kwargs: Optional[Mapping[str, Any]] = None) -> str:
        func = partial(self._scheduler.add_schedule, task, trigger, args=args, kwargs=kwargs)
        return self._portal.call(func)

    def remove_schedule(self, schedule_id: str) -> None:
        return self._portal.call(self._scheduler.remove_schedule, schedule_id)

    def shutdown(self, force: bool = False) -> None:
        self._portal.call(self._scheduler.shutdown, force)

    def subscribe(self, callback: Callable[[Event], Any]) -> None:
        self._portal.call(self._scheduler.subscribe, callback)
