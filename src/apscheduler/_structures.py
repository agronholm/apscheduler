from __future__ import annotations

from datetime import datetime, timedelta, timezone
from functools import partial
from typing import TYPE_CHECKING, Any, Callable
from uuid import UUID, uuid4

import attrs
import tenacity.stop
import tenacity.wait
from attrs.validators import instance_of

from ._converters import as_enum, as_timedelta
from ._enums import CoalescePolicy, JobOutcome
from .marshalling import callable_from_ref, callable_to_ref

if TYPE_CHECKING:
    from .abc import Serializer, Trigger


def serialize(inst, field, value):
    if isinstance(value, frozenset):
        return list(value)

    return value


@attrs.define(kw_only=True)
class Task:
    """
    Represents a callable and its surrounding configuration parameters.

    :param id: the unique identifier of this task
    :param func: the callable that is called when this task is run
    :param max_running_jobs: maximum number of instances of this task that are allowed
        to run concurrently
    :param misfire_grace_time: maximum number of seconds the run time of jobs created
        for this task are allowed to be late, compared to the scheduled run time
    """

    id: str
    func: Callable = attrs.field(eq=False, order=False)
    max_running_jobs: int | None = attrs.field(eq=False, order=False, default=None)
    misfire_grace_time: timedelta | None = attrs.field(
        eq=False, order=False, default=None
    )
    state: Any = None

    def marshal(self, serializer: Serializer) -> dict[str, Any]:
        marshalled = attrs.asdict(self, value_serializer=serialize)
        marshalled["func"] = callable_to_ref(self.func)
        marshalled["state"] = serializer.serialize(self.state) if self.state else None
        return marshalled

    @classmethod
    def unmarshal(cls, serializer: Serializer, marshalled: dict[str, Any]) -> Task:
        marshalled["func"] = callable_from_ref(marshalled["func"])
        if marshalled["state"] is not None:
            marshalled["state"] = serializer.deserialize(marshalled["state"])

        return cls(**marshalled)


@attrs.define(kw_only=True)
class Schedule:
    """
    Represents a schedule on which a task will be run.

    :param id: the unique identifier of this schedule
    :param task_id: unique identifier of the task to be run on this schedule
    :param args: positional arguments to pass to the task callable
    :param kwargs: keyword arguments to pass to the task callable
    :param coalesce: determines what to do when processing the schedule if multiple
        fire times have become due for this schedule since the last processing
    :param misfire_grace_time: maximum number of seconds the scheduled job's actual
        run time is allowed to be late, compared to the scheduled run time
    :param max_jitter: maximum number of seconds to randomly add to the scheduled
        time for each job created from this schedule
    :param tags: strings that can be used to categorize and filter the schedule and
        its derivative jobs
    :param conflict_policy: determines what to do if a schedule with the same ID
        already exists in the data store
    :param next_fire_time: the next time the task will be run
    :param last_fire_time: the last time the task was scheduled to run
    :param acquired_by: ID of the scheduler that has acquired this schedule for
        processing
    :param acquired_until: the time after which other schedulers are free to acquire the
        schedule for processing even if it is still marked as acquired
    """

    id: str
    task_id: str = attrs.field(eq=False, order=False)
    trigger: Trigger = attrs.field(eq=False, order=False)
    args: tuple = attrs.field(eq=False, order=False, converter=tuple, default=())
    kwargs: dict[str, Any] = attrs.field(
        eq=False, order=False, converter=dict, default=()
    )
    coalesce: CoalescePolicy = attrs.field(
        eq=False,
        order=False,
        default=CoalescePolicy.latest,
        converter=as_enum(CoalescePolicy),
    )
    misfire_grace_time: timedelta | None = attrs.field(
        eq=False, order=False, default=None, converter=as_timedelta
    )
    max_jitter: timedelta | None = attrs.field(
        eq=False, order=False, converter=as_timedelta, default=None
    )
    tags: frozenset[str] = attrs.field(
        eq=False, order=False, converter=frozenset, default=()
    )
    next_fire_time: datetime | None = attrs.field(eq=False, order=False, default=None)
    last_fire_time: datetime | None = attrs.field(eq=False, order=False, default=None)
    acquired_by: str | None = attrs.field(eq=False, order=False, default=None)
    acquired_until: datetime | None = attrs.field(eq=False, order=False, default=None)

    def marshal(self, serializer: Serializer) -> dict[str, Any]:
        marshalled = attrs.asdict(self, value_serializer=serialize)
        marshalled["trigger"] = serializer.serialize(self.trigger)
        marshalled["args"] = serializer.serialize(self.args)
        marshalled["kwargs"] = serializer.serialize(self.kwargs)
        if not self.acquired_by:
            del marshalled["acquired_by"]
            del marshalled["acquired_until"]

        return marshalled

    @classmethod
    def unmarshal(cls, serializer: Serializer, marshalled: dict[str, Any]) -> Schedule:
        marshalled["trigger"] = serializer.deserialize(marshalled["trigger"])
        marshalled["args"] = serializer.deserialize(marshalled["args"])
        marshalled["kwargs"] = serializer.deserialize(marshalled["kwargs"])
        return cls(**marshalled)

    @property
    def next_deadline(self) -> datetime | None:
        if self.next_fire_time and self.misfire_grace_time:
            return self.next_fire_time + self.misfire_grace_time

        return None


@attrs.define(kw_only=True)
class Job:
    """
    Represents a queued request to run a task.

    :param id: autogenerated unique identifier of the job
    :param task_id: unique identifier of the task to be run
    :param args: positional arguments to pass to the task callable
    :param kwargs: keyword arguments to pass to the task callable
    :param schedule_id: unique identifier of the associated schedule
        (if the job was derived from a schedule)
    :param scheduled_fire_time: the time the job was scheduled to run at
        (if the job was derived from a schedule; includes jitter)
    :param jitter: the time that was randomly added to the calculated scheduled run time
        (if the job was derived from a schedule)
    :param start_deadline: if the job is started in the worker after this time, it is
        considered to be misfired and will be aborted
    :param tags: strings that can be used to categorize and filter the job
    :param created_at: the time at which the job was created
    :param started_at: the time at which the execution of the job was started
    :param acquired_by: the unique identifier of the worker that has acquired the job
        for execution
    :param acquired_until: the time after which other workers are free to acquire the
        job for processing even if it is still marked as acquired
    """

    id: UUID = attrs.field(factory=uuid4)
    task_id: str = attrs.field(eq=False, order=False)
    args: tuple = attrs.field(eq=False, order=False, converter=tuple, default=())
    kwargs: dict[str, Any] = attrs.field(
        eq=False, order=False, converter=dict, default=()
    )
    schedule_id: str | None = attrs.field(eq=False, order=False, default=None)
    scheduled_fire_time: datetime | None = attrs.field(
        eq=False, order=False, default=None
    )
    jitter: timedelta = attrs.field(
        eq=False, order=False, converter=as_timedelta, factory=timedelta
    )
    start_deadline: datetime | None = attrs.field(eq=False, order=False, default=None)
    tags: frozenset[str] = attrs.field(
        eq=False, order=False, converter=frozenset, default=()
    )
    created_at: datetime = attrs.field(
        eq=False, order=False, factory=partial(datetime.now, timezone.utc)
    )
    started_at: datetime | None = attrs.field(eq=False, order=False, default=None)
    acquired_by: str | None = attrs.field(eq=False, order=False, default=None)
    acquired_until: datetime | None = attrs.field(eq=False, order=False, default=None)

    @property
    def original_scheduled_time(self) -> datetime | None:
        """The scheduled time without any jitter included."""
        if self.scheduled_fire_time is None:
            return None

        return self.scheduled_fire_time - self.jitter

    def marshal(self, serializer: Serializer) -> dict[str, Any]:
        marshalled = attrs.asdict(self, value_serializer=serialize)
        marshalled["args"] = serializer.serialize(self.args)
        marshalled["kwargs"] = serializer.serialize(self.kwargs)
        if not self.acquired_by:
            del marshalled["acquired_by"]
            del marshalled["acquired_until"]

        return marshalled

    @classmethod
    def unmarshal(cls, serializer: Serializer, marshalled: dict[str, Any]) -> Job:
        marshalled["args"] = serializer.deserialize(marshalled["args"])
        marshalled["kwargs"] = serializer.deserialize(marshalled["kwargs"])
        return cls(**marshalled)


@attrs.define(kw_only=True)
class JobInfo:
    """
    Contains information about the currently running job.

    This information is available in the thread or task where a job is currently being
    run, available from :data:`~apscheduler.current_job`.

    :param job_id: the unique identifier of the job
    :param task_id: the unique identifier of the task that is being run
    :param schedule_id: the unique identifier of the schedule that the job was derived
        from (if any)
    :param jitter: the time that was randomly added to the calculated scheduled run time
        (if the job was derived from a schedule)
    :param start_deadline: if the job is started in the worker after this time, it is
        considered to be misfired and will be aborted
    :param tags: strings that can be used to categorize and filter the job
    """

    job_id: UUID
    task_id: str
    schedule_id: str | None
    scheduled_fire_time: datetime | None
    jitter: timedelta
    start_deadline: datetime | None
    tags: frozenset[str]

    @classmethod
    def from_job(cls, job: Job) -> JobInfo:
        return cls(
            job_id=job.id,
            task_id=job.task_id,
            schedule_id=job.schedule_id,
            scheduled_fire_time=job.scheduled_fire_time,
            jitter=job.jitter,
            start_deadline=job.start_deadline,
            tags=job.tags,
        )


@attrs.define(kw_only=True, frozen=True)
class JobResult:
    """
    Represents the result of running a job.

    :param job_id: the unique identifier of the job
    :param outcome: indicates how the job ended
    :param finished_at: the time when the job ended
    :param exception: the exception object if the job ended due to an exception being
        raised
    :param return_value: the return value from the task function (if the job ran to
        completion successfully)
    """

    job_id: UUID
    outcome: JobOutcome = attrs.field(
        eq=False, order=False, converter=as_enum(JobOutcome)
    )
    finished_at: datetime = attrs.field(
        eq=False, order=False, factory=partial(datetime.now, timezone.utc)
    )
    exception: BaseException | None = attrs.field(eq=False, order=False, default=None)
    return_value: Any = attrs.field(eq=False, order=False, default=None)

    def marshal(self, serializer: Serializer) -> dict[str, Any]:
        marshalled = attrs.asdict(self, value_serializer=serialize)
        if self.outcome is JobOutcome.error:
            marshalled["exception"] = serializer.serialize(self.exception)
        else:
            del marshalled["exception"]

        if self.outcome is JobOutcome.success:
            marshalled["return_value"] = serializer.serialize(self.return_value)
        else:
            del marshalled["return_value"]

        return marshalled

    @classmethod
    def unmarshal(cls, serializer: Serializer, marshalled: dict[str, Any]) -> JobResult:
        if marshalled.get("exception"):
            marshalled["exception"] = serializer.deserialize(marshalled["exception"])
        elif marshalled.get("return_value"):
            marshalled["return_value"] = serializer.deserialize(
                marshalled["return_value"]
            )

        return cls(**marshalled)


@attrs.define(kw_only=True, frozen=True)
class RetrySettings:
    """
    Settings for retrying an operation with Tenacity.

    :param stop: defines when to stop trying
    :param wait: defines how long to wait between attempts
    """

    stop: tenacity.stop.stop_base = attrs.field(
        validator=instance_of(tenacity.stop.stop_base),
        default=tenacity.stop_after_delay(60),
    )
    wait: tenacity.wait.wait_base = attrs.field(
        validator=instance_of(tenacity.wait.wait_base),
        default=tenacity.wait_exponential(min=0.5, max=20),
    )
