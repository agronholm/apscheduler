from __future__ import annotations

from datetime import datetime, timezone
from functools import partial
from traceback import format_tb
from typing import Any
from uuid import UUID

import attrs
from attrs.converters import optional

from ._converters import as_aware_datetime, as_enum, as_uuid
from ._enums import JobOutcome
from ._structures import Job, JobResult
from ._utils import qualified_name


@attrs.define(kw_only=True, frozen=True)
class Event:
    """
    Base class for all events.

    :ivar timestamp: the time when the event occurred
    """

    timestamp: datetime = attrs.field(
        factory=partial(datetime.now, timezone.utc), converter=as_aware_datetime
    )

    def marshal(self) -> dict[str, Any]:
        return attrs.asdict(self)

    @classmethod
    def unmarshal(cls, marshalled: dict[str, Any]) -> Event:
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
    :ivar task_id: ID of the task the schedule belongs to
    :ivar next_fire_time: the first run time calculated for the schedule
    """

    schedule_id: str
    task_id: str
    next_fire_time: datetime | None = attrs.field(converter=optional(as_aware_datetime))


@attrs.define(kw_only=True, frozen=True)
class ScheduleUpdated(DataStoreEvent):
    """
    Signals that a schedule has been updated in the store.

    :ivar schedule_id: ID of the schedule that was updated
    :ivar task_id: ID of the task the schedule belongs to
    :ivar next_fire_time: the next time the schedule will run
    """

    schedule_id: str
    task_id: str
    next_fire_time: datetime | None = attrs.field(converter=optional(as_aware_datetime))


@attrs.define(kw_only=True, frozen=True)
class ScheduleRemoved(DataStoreEvent):
    """
    Signals that a schedule was removed from the store.

    :ivar schedule_id: ID of the schedule that was removed
    :ivar task_id: ID of the task the schedule belongs to
    :ivar finished: ``True`` if the schedule was removed automatically because its
        trigger had no more fire times left
    """

    schedule_id: str
    task_id: str
    finished: bool


@attrs.define(kw_only=True, frozen=True)
class JobAdded(DataStoreEvent):
    """
    Signals that a new job was added to the store.

    :ivar job_id: ID of the job that was added
    :ivar task_id: ID of the task the job would run
    :ivar schedule_id: ID of the schedule the job was created from
    """

    job_id: UUID = attrs.field(converter=as_uuid)
    task_id: str
    schedule_id: str | None


@attrs.define(kw_only=True, frozen=True)
class JobRemoved(DataStoreEvent):
    """
    Signals that a job was removed from the store.

    :ivar job_id: ID of the job that was removed
    :ivar task_id: ID of the task the job would have run

    """

    job_id: UUID = attrs.field(converter=as_uuid)
    task_id: str


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


@attrs.define(kw_only=True, frozen=True)
class JobAcquired(SchedulerEvent):
    """
    Signals that a scheduler has acquired a job for processing.

    :param job_id: the ID of the job that was acquired
    :param scheduler_id: the ID of the scheduler that acquired the job
    """

    job_id: UUID = attrs.field(converter=as_uuid)
    scheduler_id: str
    task_id: str
    schedule_id: str | None = None

    @classmethod
    def from_job(cls, job: Job, scheduler_id: str) -> JobAcquired:
        """
        Create a new job-acquired event from a job and a scheduler ID.

        :param job: the job that was acquired
        :param scheduler_id: the ID of the scheduler that acquired the job
        :return: a new job-acquired event

        """
        return cls(
            job_id=job.id,
            scheduler_id=scheduler_id,
            task_id=job.task_id,
            schedule_id=job.schedule_id,
        )


@attrs.define(kw_only=True, frozen=True)
class JobReleased(SchedulerEvent):
    """
    Signals that a scheduler has finished processing of a job.

    :param uuid.UUID job_id: the ID of the job that was released
    :param scheduler_id: the ID of the scheduler that released the job
    :param outcome: the outcome of the job
    :param exception_type: the fully qualified name of the exception if ``outcome`` is
        :attr:`JobOutcome.error`
    :param exception_message: the result of ``str(exception)`` if ``outcome`` is
        :attr:`JobOutcome.error`
    :param exception_traceback: the traceback lines from the exception if ``outcome`` is
        :attr:`JobOutcome.error`
    """

    job_id: UUID = attrs.field(converter=as_uuid)
    scheduler_id: str
    task_id: str
    schedule_id: str | None = None
    outcome: JobOutcome = attrs.field(converter=as_enum(JobOutcome))
    exception_type: str | None = None
    exception_message: str | None = None
    exception_traceback: list[str] | None = None

    @classmethod
    def from_result(cls, job: Job, result: JobResult, scheduler_id: str) -> JobReleased:
        """
        Create a new job-released event from a job, the job result and a scheduler ID.

        :param job: the job that was acquired
        :param result: the result of the job
        :param scheduler_id: the ID of the scheduler that acquired the job
        :return: a new job-released event

        """
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
            scheduler_id=scheduler_id,
            task_id=job.task_id,
            schedule_id=job.schedule_id,
            outcome=result.outcome,
            exception_type=exception_type,
            exception_message=exception_message,
            exception_traceback=exception_traceback,
        )

    def marshal(self) -> dict[str, Any]:
        marshalled = super().marshal()
        return marshalled
