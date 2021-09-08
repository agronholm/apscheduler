from __future__ import annotations

import logging
from abc import abstractmethod
from asyncio import iscoroutinefunction
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime, timezone
from functools import partial
from inspect import isawaitable
from logging import Logger
from traceback import format_tb
from typing import Any, Callable, Iterable, NewType, Optional
from uuid import UUID

import attr
from anyio import create_task_group
from anyio.abc import TaskGroup

from . import abc
from .structures import Job

SubscriptionToken = NewType('SubscriptionToken', object)


def timestamp_to_datetime(value: datetime | float | None) -> Optional[datetime]:
    if isinstance(value, float):
        return datetime.fromtimestamp(value, timezone.utc)

    return value


@attr.define(kw_only=True, frozen=True)
class Event:
    timestamp: datetime = attr.field(factory=partial(datetime.now, timezone.utc),
                                     converter=timestamp_to_datetime)


#
# Data store events
#

@attr.define(kw_only=True, frozen=True)
class DataStoreEvent(Event):
    pass


@attr.define(kw_only=True, frozen=True)
class TaskAdded(DataStoreEvent):
    task_id: str


@attr.define(kw_only=True, frozen=True)
class TaskUpdated(DataStoreEvent):
    task_id: str


@attr.define(kw_only=True, frozen=True)
class TaskRemoved(DataStoreEvent):
    task_id: str


@attr.define(kw_only=True, frozen=True)
class ScheduleAdded(DataStoreEvent):
    schedule_id: str
    next_fire_time: Optional[datetime] = attr.field(converter=timestamp_to_datetime)


@attr.define(kw_only=True, frozen=True)
class ScheduleUpdated(DataStoreEvent):
    schedule_id: str
    next_fire_time: Optional[datetime] = attr.field(converter=timestamp_to_datetime)


@attr.define(kw_only=True, frozen=True)
class ScheduleRemoved(DataStoreEvent):
    schedule_id: str


@attr.define(kw_only=True, frozen=True)
class JobAdded(DataStoreEvent):
    job_id: UUID
    task_id: str
    schedule_id: Optional[str]
    tags: frozenset[str]


@attr.define(kw_only=True, frozen=True)
class JobRemoved(DataStoreEvent):
    job_id: UUID


@attr.define(kw_only=True, frozen=True)
class ScheduleDeserializationFailed(DataStoreEvent):
    schedule_id: str
    exception: BaseException


@attr.define(kw_only=True, frozen=True)
class JobDeserializationFailed(DataStoreEvent):
    job_id: UUID
    exception: BaseException


#
# Scheduler events
#

@attr.define(kw_only=True, frozen=True)
class SchedulerEvent(Event):
    pass


@attr.define(kw_only=True, frozen=True)
class SchedulerStarted(SchedulerEvent):
    pass


@attr.define(kw_only=True, frozen=True)
class SchedulerStopped(SchedulerEvent):
    exception: Optional[BaseException] = None


#
# Worker events
#

@attr.define(kw_only=True, frozen=True)
class WorkerEvent(Event):
    pass


@attr.define(kw_only=True, frozen=True)
class WorkerStarted(WorkerEvent):
    pass


@attr.define(kw_only=True, frozen=True)
class WorkerStopped(WorkerEvent):
    exception: Optional[BaseException] = None


@attr.define(kw_only=True, frozen=True)
class JobExecutionEvent(WorkerEvent):
    job_id: UUID
    task_id: str
    schedule_id: Optional[str]
    scheduled_fire_time: Optional[datetime]
    start_deadline: Optional[datetime]


@attr.define(kw_only=True, frozen=True)
class JobStarted(JobExecutionEvent):
    """Signals that a worker has started running a job."""

    @classmethod
    def from_job(cls, job: Job, start_time: datetime) -> JobStarted:
        return JobStarted(
            timestamp=start_time, job_id=job.id, task_id=job.task_id,
            schedule_id=job.schedule_id, scheduled_fire_time=job.scheduled_fire_time,
            start_deadline=job.start_deadline)


@attr.define(kw_only=True, frozen=True)
class JobDeadlineMissed(JobExecutionEvent):
    """Signals that a worker has skipped a job because its deadline was missed."""

    @classmethod
    def from_job(cls, job: Job, start_time: datetime) -> JobDeadlineMissed:
        return JobDeadlineMissed(
            timestamp=datetime.now(timezone.utc), job_id=job.id, task_id=job.task_id,
            schedule_id=job.schedule_id, scheduled_fire_time=job.scheduled_fire_time,
            start_deadline=job.start_deadline)


@attr.define(kw_only=True, frozen=True)
class JobCompleted(JobExecutionEvent):
    """Signals that a worker has successfully run a job."""
    start_time: datetime
    return_value: str

    @classmethod
    def from_retval(cls, job: Job, start_time: datetime, return_value: Any) -> JobCompleted:
        return JobCompleted(job_id=job.id, task_id=job.task_id, schedule_id=job.schedule_id,
                            scheduled_fire_time=job.scheduled_fire_time, start_time=start_time,
                            start_deadline=job.start_deadline, return_value=repr(return_value))


@attr.define(kw_only=True, frozen=True)
class JobCancelled(JobExecutionEvent):
    """Signals that a job was cancelled."""
    start_time: datetime

    @classmethod
    def from_job(cls, job: Job, start_time: datetime) -> JobCancelled:
        return JobCancelled(job_id=job.id, task_id=job.task_id, schedule_id=job.schedule_id,
                            scheduled_fire_time=job.scheduled_fire_time, start_time=start_time,
                            start_deadline=job.start_deadline)


@attr.define(kw_only=True, frozen=True)
class JobFailed(JobExecutionEvent):
    """Signals that a worker encountered an exception while running a job."""
    start_time: datetime
    exc_type: str
    exc_val: str
    exc_tb: str

    @classmethod
    def from_exception(cls, job: Job, start_time: datetime, exception: BaseException) -> JobFailed:
        if exception.__class__.__module__ == 'builtins':
            exc_type = exception.__class__.__qualname__
        else:
            exc_type = f'{exception.__class__.__module__}.{exception.__class__.__qualname__}'

        return JobFailed(job_id=job.id, task_id=job.task_id, schedule_id=job.schedule_id,
                         scheduled_fire_time=job.scheduled_fire_time, start_time=start_time,
                         start_deadline=job.start_deadline, exc_type=exc_type,
                         exc_val=str(exception),
                         exc_tb='\n'.join(format_tb(exception.__traceback__)))


#
# Event delivery
#

@attr.define(eq=False, frozen=True)
class Subscription:
    callback: Callable[[Event], Any]
    event_types: Optional[set[type[Event]]]


@attr.define
class _BaseEventHub(abc.EventSource):
    _logger: Logger = attr.field(init=False, factory=lambda: logging.getLogger(__name__))
    _subscriptions: dict[SubscriptionToken, Subscription] = attr.field(init=False, factory=dict)

    def subscribe(self, callback: Callable[[Event], Any],
                  event_types: Optional[Iterable[type[Event]]] = None) -> SubscriptionToken:
        types = set(event_types) if event_types else None
        token = SubscriptionToken(object())
        subscription = Subscription(callback, types)
        self._subscriptions[token] = subscription
        return token

    def unsubscribe(self, token: SubscriptionToken) -> None:
        self._subscriptions.pop(token, None)

    @abstractmethod
    def publish(self, event: Event) -> None:
        """Publish an event to all subscribers."""

    def relay_events_from(self, source: abc.EventSource) -> SubscriptionToken:
        return source.subscribe(self.publish)


class EventHub(_BaseEventHub):
    _executor: ThreadPoolExecutor

    def __enter__(self) -> EventHub:
        self._executor = ThreadPoolExecutor(1)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._executor.shutdown(wait=exc_type is None)

    def subscribe(self, callback: Callable[[Event], Any],
                  event_types: Optional[Iterable[type[Event]]] = None) -> SubscriptionToken:
        if iscoroutinefunction(callback):
            raise ValueError('Coroutine functions are not supported as callbacks on a synchronous '
                             'event source')

        return super().subscribe(callback, event_types)

    def publish(self, event: Event) -> None:
        def deliver_event(func: Callable[[Event], Any]) -> None:
            try:
                func(event)
            except BaseException:
                self._logger.exception('Error delivering %s event', event.__class__.__name__)

        event_type = type(event)
        for subscription in list(self._subscriptions.values()):
            if subscription.event_types is None or event_type in subscription.event_types:
                self._executor.submit(deliver_event, subscription.callback)


class AsyncEventHub(_BaseEventHub):
    _task_group: TaskGroup

    async def __aenter__(self) -> AsyncEventHub:
        self._task_group = create_task_group()
        await self._task_group.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._task_group.__aexit__(exc_type, exc_val, exc_tb)
        del self._task_group

    def publish(self, event: Event) -> None:
        async def deliver_event(func: Callable[[Event], Any]) -> None:
            try:
                retval = func(event)
                if isawaitable(retval):
                    await retval
            except BaseException:
                self._logger.exception('Error delivering %s event', event.__class__.__name__)

        event_type = type(event)
        for subscription in self._subscriptions.values():
            if subscription.event_types is None or event_type in subscription.event_types:
                self._task_group.start_soon(deliver_event, subscription.callback)
