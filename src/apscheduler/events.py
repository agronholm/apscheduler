from __future__ import annotations

from datetime import datetime, timezone
from functools import partial
from typing import Optional
from uuid import UUID

import attr
from attr.converters import optional

from .converters import as_aware_datetime, as_uuid
from .enums import JobOutcome
from .structures import Job


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
    """Signals that a worker has started processing a job."""

    @classmethod
    def from_job(cls, job: Job, start_time: datetime) -> JobStarted:
        return JobStarted(
            timestamp=start_time, job_id=job.id, task_id=job.task_id,
            schedule_id=job.schedule_id, scheduled_fire_time=job.scheduled_fire_time,
            start_deadline=job.start_deadline)


@attr.define(kw_only=True, frozen=True)
class JobEnded(JobExecutionEvent):
    """Signals that a worker has finished processing of a job."""

    outcome: JobOutcome
    start_time: datetime = attr.field(converter=as_aware_datetime)

    @classmethod
    def from_job(cls, job: Job, outcome: JobOutcome, start_time: datetime) -> JobEnded:
        return JobEnded(
            timestamp=datetime.now(timezone.utc), job_id=job.id, task_id=job.task_id,
            schedule_id=job.schedule_id, scheduled_fire_time=job.scheduled_fire_time,
            start_deadline=job.start_deadline, outcome=outcome, start_time=start_time)
