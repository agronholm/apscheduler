from collections import defaultdict
from contextlib import ExitStack, suppress
from dataclasses import dataclass, field
from datetime import datetime
from inspect import isawaitable, isclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Type
from uuid import UUID
from warnings import warn

from anyio import run_sync_in_worker_thread, start_blocking_portal, to_thread
from anyio.abc import BlockingPortal

from .abc import Event, EventSource


@dataclass(frozen=True)
class JobEvent(Event):
    job_id: UUID
    task_id: str
    schedule_id: Optional[str]


@dataclass(frozen=True)
class JobAdded(JobEvent):
    pass


@dataclass(frozen=True)
class JobUpdated(JobEvent):
    pass


@dataclass(frozen=True)
class JobExecutionEvent(JobEvent):
    job_id: UUID
    task_id: str
    schedule_id: Optional[str]
    scheduled_fire_time: Optional[datetime]
    start_time: datetime
    start_deadline: datetime


@dataclass(frozen=True)
class JobDeadlineMissed(JobExecutionEvent):
    pass


@dataclass(frozen=True)
class JobStarted(JobExecutionEvent):
    pass


@dataclass(frozen=True)
class JobSuccessful(JobExecutionEvent):
    return_value: Any


@dataclass(frozen=True)
class JobFailed(JobExecutionEvent):
    traceback: str
    exception: Optional[BaseException] = None


@dataclass(frozen=True)
class ScheduleEvent(Event):
    schedule_id: str


@dataclass(frozen=True)
class ScheduleAdded(ScheduleEvent):
    next_fire_time: Optional[datetime]


@dataclass(frozen=True)
class ScheduleUpdated(ScheduleEvent):
    next_fire_time: Optional[datetime]


@dataclass(frozen=True)
class ScheduleRemoved(ScheduleEvent):
    pass


_all_event_types = [x for x in locals().values() if isclass(x) and issubclass(x, Event)]


#
# Event delivery
#

@dataclass
class EventHub(EventSource):
    _subscribers: Dict[Type[Event], List[Callable[[Event], Any]]] = field(
        init=False, default_factory=lambda: defaultdict(list))

    def subscribe(self, callback: Callable[[Event], Any],
                  event_types: Optional[Iterable[Type[Event]]] = None) -> None:
        if event_types is None:
            event_types = _all_event_types

        for event_type in event_types:
            existing_callbacks = self._subscribers[event_type]
            if callback not in existing_callbacks:
                existing_callbacks.append(callback)

    def unsubscribe(self, callback: Callable[[Event], Any],
                    event_types: Optional[Iterable[Type[Event]]] = None) -> None:
        if event_types is None:
            event_types = _all_event_types

        for event_type in event_types:
            existing_callbacks = self._subscribers.get(event_type, [])
            with suppress(ValueError):
                existing_callbacks.remove(callback)

    async def publish(self, event: Event) -> None:
        for callback in self._subscribers[type(event)]:
            try:
                retval = callback(event)
                if isawaitable(retval):
                    await retval
            except Exception as exc:
                warn(f'Failed to deliver {event.__class__.__name__} event to callback '
                     f'{callback!r}: {exc.__class__.__name__}: {exc}')


class SyncEventSource:
    _subscribers: Dict[Type[Event], List[Callable[[Event], Any]]]

    def __init__(self, async_event_source: EventSource, portal: Optional[BlockingPortal] = None):
        self.portal = portal
        self._async_event_source = async_event_source
        self._async_event_source.subscribe(self._forward_async_event)
        self._exit_stack = ExitStack()
        self._subscribers = defaultdict(list)

    def __enter__(self):
        self._exit_stack.__enter__()
        if not self.portal:
            portal_cm = start_blocking_portal()
            self.portal = self._exit_stack.enter_context(portal_cm)
            self._async_event_source.subscribe(self._forward_async_event)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._exit_stack.__exit__(exc_type, exc_val, exc_tb)

    async def _forward_async_event(self, event: Event) -> None:
        for subscriber in self._subscribers.get(type(event), ()):
            await to_thread.run_sync(subscriber, event)

    def subscribe(self, callback: Callable[[Event], Any],
                  event_types: Optional[Iterable[Type[Event]]] = None) -> None:
        if event_types is None:
            event_types = _all_event_types

        for event_type in event_types:
            existing_callbacks = self._subscribers[event_type]
            if callback not in existing_callbacks:
                existing_callbacks.append(callback)

    def unsubscribe(self, callback: Callable[[Event], Any],
                    event_types: Optional[Iterable[Type[Event]]] = None) -> None:
        if event_types is None:
            event_types = _all_event_types

        for event_type in event_types:
            existing_callbacks = self._subscribers.get(event_type, [])
            with suppress(ValueError):
                existing_callbacks.remove(callback)
