from __future__ import annotations

from datetime import datetime, timedelta, timezone
from functools import partial
from typing import Any
from uuid import UUID, uuid4

import attrs
from attrs.validators import and_, gt, instance_of, matches_re, min_len, optional

from ._converters import as_aware_datetime, as_enum, as_timedelta
from ._enums import CoalescePolicy, JobOutcome
from .abc import Serializer, Trigger


def serialize(inst: Any, field: attrs.Attribute, value: Any) -> Any:
    if isinstance(value, frozenset):
        return list(value)

    return value


@attrs.define(kw_only=True)
class Task:
    """
    Represents a callable and its surrounding configuration parameters.

    :var str id: the unique identifier of this task
    :var ~collections.abc.Callable func: the callable that is called when this task is
        run
    :var str job_executor: name of the job executor that will run this task
    :var int | None max_running_jobs: maximum number of instances of this task that are
        allowed to run concurrently
    :var ~datetime.timedelta | None misfire_grace_time: maximum number of seconds the
        run time of jobs created for this task are allowed to be late, compared to the
        scheduled run time
    """

    id: str = attrs.field(validator=[instance_of(str), min_len(1)])
    func: str | None = attrs.field(
        eq=False,
        order=False,
        validator=optional(and_(instance_of(str), matches_re(r".+:.+"))),
    )
    job_executor: str = attrs.field(eq=False, validator=instance_of(str))
    max_running_jobs: int | None = attrs.field(
        eq=False,
        order=False,
        default=None,
        validator=optional(and_(instance_of(int), gt(0))),
    )
    misfire_grace_time: timedelta | None = attrs.field(
        eq=False,
        order=False,
        default=None,
        converter=as_timedelta,
        validator=optional(instance_of(timedelta)),
    )

    def marshal(self, serializer: Serializer) -> dict[str, Any]:
        return attrs.asdict(self, value_serializer=serialize)

    @classmethod
    def unmarshal(cls, serializer: Serializer, marshalled: dict[str, Any]) -> Task:
        return cls(**marshalled)


@attrs.define(kw_only=True)
class Schedule:
    """
    Represents a schedule on which a task will be run.

    :var str id: the unique identifier of this schedule
    :var str task_id: unique identifier of the task to be run on this schedule
    :var tuple args: positional arguments to pass to the task callable
    :var dict[str, Any] kwargs: keyword arguments to pass to the task callable
    :var CoalescePolicy coalesce: determines what to do when processing the schedule if
        multiple fire times have become due for this schedule since the last processing
    :var ~datetime.timedelta | None misfire_grace_time: maximum number of seconds the
        scheduled job's actual run time is allowed to be late, compared to the scheduled
        run time
    :var ~datetime.timedelta | None max_jitter: maximum number of seconds to randomly
        add to the scheduled time for each job created from this schedule
    :var ConflictPolicy conflict_policy: determines what to do if a schedule with the
        same ID already exists in the data store
    :var ~datetime.datetime next_fire_time: the next time the task will be run
    :var ~datetime.datetime | None last_fire_time: the last time the task was scheduled
        to run
    :var str | None acquired_by: ID of the scheduler that has acquired this schedule for
        processing
    :var str | None acquired_until: the time after which other schedulers are free to
        acquire the schedule for processing even if it is still marked as acquired
    """

    id: str = attrs.field(validator=[instance_of(str), min_len(1)])
    task_id: str = attrs.field(
        eq=False, order=False, validator=[instance_of(str), min_len(1)]
    )
    trigger: Trigger = attrs.field(
        eq=False,
        order=False,
        validator=instance_of(Trigger),  # type: ignore[type-abstract]
    )
    args: tuple = attrs.field(eq=False, order=False, converter=tuple, default=())
    kwargs: dict[str, Any] = attrs.field(
        eq=False, order=False, converter=dict, default=()
    )
    coalesce: CoalescePolicy = attrs.field(
        eq=False,
        order=False,
        default=CoalescePolicy.latest,
        converter=as_enum(CoalescePolicy),
        validator=instance_of(CoalescePolicy),
    )
    misfire_grace_time: timedelta | None = attrs.field(
        eq=False,
        order=False,
        default=None,
        converter=as_timedelta,
        validator=optional(instance_of(timedelta)),
    )
    max_jitter: timedelta | None = attrs.field(
        eq=False,
        order=False,
        converter=as_timedelta,
        default=None,
        validator=optional(instance_of(timedelta)),
    )
    next_fire_time: datetime | None = attrs.field(
        eq=False, order=False, converter=as_aware_datetime, default=None
    )
    last_fire_time: datetime | None = attrs.field(
        eq=False, order=False, converter=as_aware_datetime, default=None
    )
    acquired_by: str | None = attrs.field(eq=False, order=False, default=None)
    acquired_until: datetime | None = attrs.field(
        eq=False, order=False, converter=as_aware_datetime, default=None
    )

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


@attrs.define(kw_only=True, frozen=True)
class Job:
    """
    Represents a queued request to run a task.

    :var ~uuid.UUID id: autogenerated unique identifier of the job
    :var str task_id: unique identifier of the task to be run
    :var tuple args: positional arguments to pass to the task callable
    :var dict[str, Any] kwargs: keyword arguments to pass to the task callable
    :var str schedule_id: unique identifier of the associated schedule
        (if the job was derived from a schedule)
    :var ~datetime.datetime | None scheduled_fire_time: the time the job was scheduled
        to run at (if the job was derived from a schedule; includes jitter)
    :var ~datetime.timedelta | None jitter: the time that was randomly added to the
        calculated scheduled run time (if the job was derived from a schedule)
    :var ~datetime.datetime | None start_deadline: if the job is started in the
        scheduler after this time, it is considered to be misfired and will be aborted
    :var ~datetime.timedelta result_expiration_time: minimum amount of time to keep the
        result available for fetching in the data store
    :var ~datetime.datetime created_at: the time at which the job was created
    :var ~datetime.datetime | None started_at: the time at which the execution of the
        job was started
    :var str | None acquired_by: the unique identifier of the scheduler that has
        acquired the job for execution
    :var str | None acquired_until: the time after which other schedulers are free to
        acquire the job for processing even if it is still marked as acquired
    """

    id: UUID = attrs.field(factory=uuid4)
    task_id: str = attrs.field(eq=False, order=False)
    args: tuple = attrs.field(eq=False, order=False, converter=tuple, default=())
    kwargs: dict[str, Any] = attrs.field(
        eq=False, order=False, converter=dict, default=()
    )
    schedule_id: str | None = attrs.field(eq=False, order=False, default=None)
    scheduled_fire_time: datetime | None = attrs.field(
        eq=False, order=False, converter=as_aware_datetime, default=None
    )
    jitter: timedelta = attrs.field(
        eq=False, order=False, converter=as_timedelta, factory=timedelta
    )
    start_deadline: datetime | None = attrs.field(
        eq=False, order=False, converter=as_aware_datetime, default=None
    )
    result_expiration_time: timedelta = attrs.field(
        eq=False, order=False, converter=as_timedelta, default=timedelta()
    )
    created_at: datetime = attrs.field(
        eq=False,
        order=False,
        converter=as_aware_datetime,
        factory=partial(datetime.now, timezone.utc),
    )
    started_at: datetime | None = attrs.field(
        eq=False, order=False, converter=as_aware_datetime, default=None
    )
    acquired_by: str | None = attrs.field(eq=False, order=False, default=None)
    acquired_until: datetime | None = attrs.field(
        eq=False, order=False, converter=as_aware_datetime, default=None
    )

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


@attrs.define(kw_only=True, frozen=True)
class JobResult:
    """
    Represents the result of running a job.

    :var ~uuid.UUID job_id: the unique identifier of the job
    :var JobOutcome outcome: indicates how the job ended
    :var ~datetime.datetime finished_at: the time when the job ended
    :var BaseException | None exception: the exception object if the job ended due to an
        exception being raised
    :var return_value: the return value from the task function (if the job ran to
        completion successfully)
    """

    job_id: UUID
    outcome: JobOutcome = attrs.field(
        eq=False, order=False, converter=as_enum(JobOutcome)
    )
    finished_at: datetime = attrs.field(
        eq=False,
        order=False,
        converter=as_aware_datetime,
        factory=partial(datetime.now, timezone.utc),
    )
    expires_at: datetime = attrs.field(
        eq=False, converter=as_aware_datetime, order=False
    )
    exception: BaseException | None = attrs.field(eq=False, order=False, default=None)
    return_value: Any = attrs.field(eq=False, order=False, default=None)

    @classmethod
    def from_job(
        cls,
        job: Job,
        outcome: JobOutcome,
        *,
        finished_at: datetime | None = None,
        exception: BaseException | None = None,
        return_value: Any = None,
    ) -> JobResult:
        real_finished_at = finished_at or datetime.now(timezone.utc)
        expires_at = real_finished_at + job.result_expiration_time
        return cls(
            job_id=job.id,
            outcome=outcome,
            finished_at=real_finished_at,
            expires_at=expires_at,
            exception=exception,
            return_value=return_value,
        )

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
