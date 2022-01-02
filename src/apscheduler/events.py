from __future__ import annotations

from datetime import datetime, timezone
from functools import partial
from uuid import UUID

import attrs
from attrs.converters import optional

from .converters import as_aware_datetime, as_uuid
from .enums import JobOutcome


@attrs.define(kw_only=True, frozen=True)
class Event:
    timestamp: datetime = attrs.field(factory=partial(datetime.now, timezone.utc),
                                      converter=as_aware_datetime)


#
# Data store events
#

@attrs.define(kw_only=True, frozen=True)
class DataStoreEvent(Event):
    pass


@attrs.define(kw_only=True, frozen=True)
class TaskAdded(DataStoreEvent):
    task_id: str


@attrs.define(kw_only=True, frozen=True)
class TaskUpdated(DataStoreEvent):
    task_id: str


@attrs.define(kw_only=True, frozen=True)
class TaskRemoved(DataStoreEvent):
    task_id: str


@attrs.define(kw_only=True, frozen=True)
class ScheduleAdded(DataStoreEvent):
    schedule_id: str
    next_fire_time: datetime | None = attrs.field(converter=optional(as_aware_datetime))


@attrs.define(kw_only=True, frozen=True)
class ScheduleUpdated(DataStoreEvent):
    schedule_id: str
    next_fire_time: datetime | None = attrs.field(converter=optional(as_aware_datetime))


@attrs.define(kw_only=True, frozen=True)
class ScheduleRemoved(DataStoreEvent):
    schedule_id: str


@attrs.define(kw_only=True, frozen=True)
class JobAdded(DataStoreEvent):
    job_id: UUID = attrs.field(converter=as_uuid)
    task_id: str
    schedule_id: str | None
    tags: frozenset[str] = attrs.field(converter=frozenset)


@attrs.define(kw_only=True, frozen=True)
class JobRemoved(DataStoreEvent):
    job_id: UUID = attrs.field(converter=as_uuid)


@attrs.define(kw_only=True, frozen=True)
class ScheduleDeserializationFailed(DataStoreEvent):
    schedule_id: str
    exception: BaseException


@attrs.define(kw_only=True, frozen=True)
class JobDeserializationFailed(DataStoreEvent):
    job_id: UUID = attrs.field(converter=as_uuid)
    exception: BaseException


#
# Scheduler events
#

@attrs.define(kw_only=True, frozen=True)
class SchedulerEvent(Event):
    pass


@attrs.define(kw_only=True, frozen=True)
class SchedulerStarted(SchedulerEvent):
    pass


@attrs.define(kw_only=True, frozen=True)
class SchedulerStopped(SchedulerEvent):
    exception: BaseException | None = None


#
# Worker events
#

@attrs.define(kw_only=True, frozen=True)
class WorkerEvent(Event):
    pass


@attrs.define(kw_only=True, frozen=True)
class WorkerStarted(WorkerEvent):
    pass


@attrs.define(kw_only=True, frozen=True)
class WorkerStopped(WorkerEvent):
    exception: BaseException | None = None


@attrs.define(kw_only=True, frozen=True)
class JobAcquired(WorkerEvent):
    """Signals that a worker has acquired a job for processing."""

    job_id: UUID = attrs.field(converter=as_uuid)
    worker_id: str


@attrs.define(kw_only=True, frozen=True)
class JobReleased(WorkerEvent):
    """Signals that a worker has finished processing of a job."""

    job_id: UUID = attrs.field(converter=as_uuid)
    worker_id: str
    outcome: JobOutcome
