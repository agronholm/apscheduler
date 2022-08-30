from __future__ import annotations

from datetime import datetime, timezone
from functools import partial
from traceback import format_tb
from typing import Any
from uuid import UUID

import attrs
from attrs.converters import optional

from . import abc
from ._converters import as_aware_datetime, as_enum, as_uuid
from ._enums import JobOutcome
from ._structures import JobResult
from ._utils import qualified_name


def serialize(inst, field, value):
    if isinstance(value, frozenset):
        return list(value)
    elif isinstance(value, JobOutcome):
        return value.name

    return value


@attrs.define(kw_only=True, frozen=True)
class Event:
    """
    Base class for all events.

    :ivar timestamp: the time when the event occurrent
    """

    timestamp: datetime = attrs.field(
        factory=partial(datetime.now, timezone.utc), converter=as_aware_datetime
    )

    def marshal(self, serializer: abc.Serializer) -> dict[str, Any]:
        return attrs.asdict(self, value_serializer=serialize)

    @classmethod
    def unmarshal(cls, serializer: abc.Serializer, marshalled: dict[str, Any]) -> Event:
        return cls(**marshalled)


#
# Data store events
#


@attrs.define(kw_only=True, frozen=True)
class DataStoreEvent(Event):
    """Base class for events originating from a data store."""


@attrs.define(kw_only=True, frozen=True)
class TaskAdded(DataStoreEvent):
    """
    Signals that a new task was added to the store.

    :ivar task_id: ID of the task that was added
    """

    task_id: str


@attrs.define(kw_only=True, frozen=True)
class TaskUpdated(DataStoreEvent):
    """
    Signals that a task was updated in a data store.

    :ivar task_id: ID of the task that was updated
    """

    task_id: str


@attrs.define(kw_only=True, frozen=True)
class TaskRemoved(DataStoreEvent):
    """
    Signals that a task was removed from the store.

    :ivar task_id: ID of the task that was removed
    """

    task_id: str


@attrs.define(kw_only=True, frozen=True)
class ScheduleAdded(DataStoreEvent):
    """
    Signals that a new schedule was added to the store.

    :ivar schedule_id: ID of the schedule that was added
    :ivar next_fire_time: the first run time calculated for the schedule
    """

    schedule_id: str
    next_fire_time: datetime | None = attrs.field(converter=optional(as_aware_datetime))


@attrs.define(kw_only=True, frozen=True)
class ScheduleUpdated(DataStoreEvent):
    """
    Signals that a schedule has been updated in the store.

    :ivar schedule_id: ID of the schedule that was updated
    :ivar next_fire_time: the next time the schedule will run
    """

    schedule_id: str
    next_fire_time: datetime | None = attrs.field(converter=optional(as_aware_datetime))


@attrs.define(kw_only=True, frozen=True)
class ScheduleRemoved(DataStoreEvent):
    """
    Signals that a schedule was removed from the store.

    :ivar schedule_id: ID of the schedule that was removed
    """

    schedule_id: str


@attrs.define(kw_only=True, frozen=True)
class JobAdded(DataStoreEvent):
    """
    Signals that a new job was added to the store.

    :ivar job_id: ID of the job that was added
    :ivar task_id: ID of the task the job would run
    :ivar schedule_id: ID of the schedule the job was created from
    :ivar tags: the set of tags collected from the associated task and schedule
    """

    job_id: UUID = attrs.field(converter=as_uuid)
    task_id: str
    schedule_id: str | None
    tags: frozenset[str] = attrs.field(converter=frozenset)


@attrs.define(kw_only=True, frozen=True)
class JobRemoved(DataStoreEvent):
    """
    Signals that a job was removed from the store.

    :ivar job_id: ID of the job that was removed

    """

    job_id: UUID = attrs.field(converter=as_uuid)


@attrs.define(kw_only=True, frozen=True)
class ScheduleDeserializationFailed(DataStoreEvent):
    """
    Signals that the deserialization of a schedule has failed.

    :ivar schedule_id: ID of the schedule that failed to deserialize
    :ivar exception: the exception that was raised during deserialization
    """

    schedule_id: str
    exception: BaseException


@attrs.define(kw_only=True, frozen=True)
class JobDeserializationFailed(DataStoreEvent):
    """
    Signals that the deserialization of a job has failed.

    :ivar job_id: ID of the job that failed to deserialize
    :ivar exception: the exception that was raised during deserialization
    """

    job_id: UUID = attrs.field(converter=as_uuid)
    exception: BaseException


#
# Scheduler events
#


@attrs.define(kw_only=True, frozen=True)
class SchedulerEvent(Event):
    """Base class for events originating from a scheduler."""


@attrs.define(kw_only=True, frozen=True)
class SchedulerStarted(SchedulerEvent):
    pass


@attrs.define(kw_only=True, frozen=True)
class SchedulerStopped(SchedulerEvent):
    """
    Signals that a scheduler has stopped.

    :ivar exception: the exception that caused the scheduler to stop, if any
    """

    exception: BaseException | None = None


#
# Worker events
#


@attrs.define(kw_only=True, frozen=True)
class WorkerEvent(Event):
    """Base class for events originating from a worker."""


@attrs.define(kw_only=True, frozen=True)
class WorkerStarted(WorkerEvent):
    """Signals that a worker has started."""


@attrs.define(kw_only=True, frozen=True)
class WorkerStopped(WorkerEvent):
    """
    Signals that a worker has stopped.

    :ivar exception: the exception that caused the worker to stop, if any
    """

    exception: BaseException | None = None


@attrs.define(kw_only=True, frozen=True)
class JobAcquired(WorkerEvent):
    """
    Signals that a worker has acquired a job for processing.

    :param job_id: the ID of the job that was acquired
    :param worker_id: the ID of the worker that acquired the job
    """

    job_id: UUID = attrs.field(converter=as_uuid)
    worker_id: str


@attrs.define(kw_only=True, frozen=True)
class JobReleased(WorkerEvent):
    """
    Signals that a worker has finished processing of a job.

    :param job_id: the ID of the job that was released
    :param worker_id: the ID of the worker that released the job
    :param outcome: the outcome of the job
    :param exception_type: the fully qualified name of the exception if ``outcome`` is
        :data:`JobOutcome.error`
    :param exception_message: the result of ``str(exception)`` if ``outcome`` is
        :data:`JobOutcome.error`
    :param exception_traceback: the traceback lines from the exception if ``outcome`` is
        :data:`JobOutcome.error`
    """

    job_id: UUID = attrs.field(converter=as_uuid)
    worker_id: str
    outcome: JobOutcome = attrs.field(converter=as_enum(JobOutcome))
    exception_type: str | None = None
    exception_message: str | None = None
    exception_traceback: list[str] | None = None

    @classmethod
    def from_result(cls, result: JobResult, worker_id: str) -> JobReleased:
        if result.exception is not None:
            exception_type: str | None = qualified_name(result.exception.__class__)
            exception_message: str | None = str(result.exception)
            exception_traceback: list[str] | None = format_tb(
                result.exception.__traceback__
            )
        else:
            exception_type = exception_message = exception_traceback = None

        return cls(
            job_id=result.job_id,
            worker_id=worker_id,
            outcome=result.outcome,
            exception_type=exception_type,
            exception_message=exception_message,
            exception_traceback=exception_traceback,
        )

    def marshal(self, serializer: abc.Serializer) -> dict[str, Any]:
        marshalled = super().marshal(serializer)
        return marshalled
