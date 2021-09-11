from __future__ import annotations

from datetime import datetime, timezone
from functools import partial
from traceback import format_tb
from typing import Any, Callable, NewType, Optional
from uuid import UUID

import attr
from attr.converters import optional

from .converters import as_aware_datetime, as_uuid
from .structures import Job

SubscriptionToken = NewType('SubscriptionToken', object)


@attr.define(kw_only=True, frozen=True)
class Event:
    timestamp: datetime = attr.field(factory=partial(datetime.now, timezone.utc),
                                     converter=as_aware_datetime)


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
    next_fire_time: Optional[datetime] = attr.field(converter=optional(as_aware_datetime))


@attr.define(kw_only=True, frozen=True)
class ScheduleUpdated(DataStoreEvent):
    schedule_id: str
    next_fire_time: Optional[datetime] = attr.field(converter=optional(as_aware_datetime))


@attr.define(kw_only=True, frozen=True)
class ScheduleRemoved(DataStoreEvent):
    schedule_id: str


@attr.define(kw_only=True, frozen=True)
class JobAdded(DataStoreEvent):
    job_id: UUID = attr.field(converter=as_uuid)
    task_id: str
    schedule_id: Optional[str]
    tags: frozenset[str] = attr.field(converter=frozenset)


@attr.define(kw_only=True, frozen=True)
class JobRemoved(DataStoreEvent):
    job_id: UUID = attr.field(converter=as_uuid)


@attr.define(kw_only=True, frozen=True)
class ScheduleDeserializationFailed(DataStoreEvent):
    schedule_id: str
    exception: BaseException


@attr.define(kw_only=True, frozen=True)
class JobDeserializationFailed(DataStoreEvent):
    job_id: UUID = attr.field(converter=as_uuid)
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
    job_id: UUID = attr.field(converter=as_uuid)
    task_id: str
    schedule_id: Optional[str]
    scheduled_fire_time: Optional[datetime] = attr.field(converter=optional(as_aware_datetime))
    start_deadline: Optional[datetime] = attr.field(converter=optional(as_aware_datetime))


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
    start_time: datetime = attr.field(converter=optional(as_aware_datetime))
    return_value: str

    @classmethod
    def from_retval(cls, job: Job, start_time: datetime, return_value: Any) -> JobCompleted:
        return JobCompleted(job_id=job.id, task_id=job.task_id, schedule_id=job.schedule_id,
                            scheduled_fire_time=job.scheduled_fire_time, start_time=start_time,
                            start_deadline=job.start_deadline, return_value=repr(return_value))


@attr.define(kw_only=True, frozen=True)
class JobCancelled(JobExecutionEvent):
    """Signals that a job was cancelled."""
    start_time: datetime = attr.field(converter=optional(as_aware_datetime))

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
