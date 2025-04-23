from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import Any
from uuid import UUID, uuid4

import attrs
from attr.setters import frozen
from attrs.validators import and_, gt, instance_of, matches_re, min_len, optional

from ._converters import as_aware_datetime, as_enum, as_timedelta
from ._enums import CoalescePolicy, JobOutcome
from ._utils import UnsetValue, unset
from ._validators import if_not_unset, valid_metadata
from .abc import Serializer, Trigger

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

MetadataType: TypeAlias = (
    "dict[str, str | int | bool | None | list[MetadataType] | dict[str, MetadataType]]"
)


def serialize(inst: Any, field: attrs.Attribute, value: Any) -> Any:
    if isinstance(value, frozenset):
        return list(value)

    return value


@attrs.define(kw_only=True, order=False)
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
    :var metadata: key-value pairs for storing JSON compatible custom information
    """

    id: str = attrs.field(validator=[instance_of(str), min_len(1)], on_setattr=frozen)
    func: str | None = attrs.field(
        validator=optional(and_(instance_of(str), matches_re(r".+:.+"))),
        on_setattr=frozen,
    )
    job_executor: str = attrs.field(validator=instance_of(str), on_setattr=frozen)
    max_running_jobs: int | None = attrs.field(
        default=None,
        validator=optional(and_(instance_of(int), gt(0))),
        on_setattr=frozen,
    )
    misfire_grace_time: timedelta | None = attrs.field(
        default=None,
        converter=as_timedelta,
        validator=optional(instance_of(timedelta)),
        on_setattr=frozen,
    )
    metadata: MetadataType = attrs.field(validator=valid_metadata, factory=dict)
    running_jobs: int = attrs.field(default=0)

    def marshal(self, serializer: Serializer) -> dict[str, Any]:
        return attrs.asdict(self, value_serializer=serialize)

    @classmethod
    def unmarshal(cls, serializer: Serializer, marshalled: dict[str, Any]) -> Task:
        return cls(**marshalled)

    def __hash__(self) -> int:
        return hash(self.id)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Task):
            return self.id == other.id

        return NotImplemented

    def __lt__(self, other: object) -> bool:
        if isinstance(other, Task):
            return self.id < other.id

        return NotImplemented


@attrs.define(kw_only=True)
class TaskDefaults:
    """
    Contains default values for tasks that will be applied when no matching
    configuration value has been explicitly provided.

    :param str job_executor: name of the job executor that will run this task
    :param int | None max_running_jobs: maximum number of instances of this task that are
        allowed to run concurrently
    :param ~datetime.timedelta | None misfire_grace_time: maximum number of seconds the
        run time of jobs created for this task are allowed to be late, compared to the
        scheduled run time
    :var metadata: key-value pairs for storing JSON compatible custom information
    """

    job_executor: str | UnsetValue = attrs.field(
        validator=if_not_unset(instance_of(str)), default=unset
    )
    max_running_jobs: int | None | UnsetValue = attrs.field(
        validator=optional(instance_of(int)), default=1
    )
    misfire_grace_time: timedelta | None = attrs.field(
        converter=as_timedelta,
        validator=optional(instance_of(timedelta)),
        default=None,
    )
    metadata: MetadataType = attrs.field(validator=valid_metadata, factory=dict)


@attrs.define(kw_only=True, order=False)
class Schedule:
    """
    Represents a schedule on which a task will be run.

    :var str id: the unique identifier of this schedule
    :var str task_id: unique identifier of the task to be run on this schedule
    :var Trigger trigger: the trigger that determines when the task will be run
    :var tuple args: positional arguments to pass to the task callable
    :var dict[str, Any] kwargs: keyword arguments to pass to the task callable
    :var bool paused: whether the schedule is paused
    :var CoalescePolicy coalesce: determines what to do when processing the schedule if
        multiple fire times have become due for this schedule since the last processing
    :var ~datetime.timedelta | None misfire_grace_time: maximum number of seconds the
        scheduled job's actual run time is allowed to be late, compared to the scheduled
        run time
    :var ~datetime.timedelta | None max_jitter: maximum number of seconds to randomly
        add to the scheduled time for each job created from this schedule
    :var ~datetime.timedelta job_result_expiration_time: minimum time to keep the job
        results in storage from the jobs created by this schedule
    :var metadata: key-value pairs for storing JSON compatible custom information
    :var ~datetime.datetime next_fire_time: the next time the task will be run
    :var ~datetime.datetime | None last_fire_time: the last time the task was scheduled
        to run
    :var str | None acquired_by: ID of the scheduler that has acquired this schedule for
        processing
    :var str | None acquired_until: the time after which other schedulers are free to
        acquire the schedule for processing even if it is still marked as acquired
    """

    id: str = attrs.field(validator=[instance_of(str), min_len(1)], on_setattr=frozen)
    task_id: str = attrs.field(
        validator=[instance_of(str), min_len(1)], on_setattr=frozen
    )
    trigger: Trigger = attrs.field(
        validator=instance_of(Trigger),  # type: ignore[type-abstract]
        on_setattr=frozen,
    )
    args: tuple = attrs.field(converter=tuple, default=())
    kwargs: dict[str, Any] = attrs.field(converter=dict, default=())
    paused: bool = attrs.field(default=False)
    coalesce: CoalescePolicy = attrs.field(
        default=CoalescePolicy.latest,
        converter=as_enum(CoalescePolicy),
        validator=instance_of(CoalescePolicy),
        on_setattr=frozen,
    )
    misfire_grace_time: timedelta | None = attrs.field(
        default=None,
        converter=as_timedelta,
        validator=optional(instance_of(timedelta)),
        on_setattr=frozen,
    )
    max_jitter: timedelta | None = attrs.field(
        converter=as_timedelta,
        default=None,
        validator=optional(instance_of(timedelta)),
        on_setattr=frozen,
    )
    job_executor: str = attrs.field(validator=instance_of(str), on_setattr=frozen)
    job_result_expiration_time: timedelta = attrs.field(
        default=0,
        converter=as_timedelta,
        validator=optional(instance_of(timedelta)),
        on_setattr=frozen,
    )
    metadata: MetadataType = attrs.field(validator=valid_metadata, factory=dict)
    next_fire_time: datetime | None = attrs.field(
        converter=as_aware_datetime,
        default=None,
    )
    last_fire_time: datetime | None = attrs.field(
        converter=as_aware_datetime,
        default=None,
    )
    acquired_by: str | None = attrs.field(default=None)
    acquired_until: datetime | None = attrs.field(
        converter=as_aware_datetime, default=None
    )

    def marshal(self, serializer: Serializer) -> dict[str, Any]:
        marshalled = attrs.asdict(self, recurse=False, value_serializer=serialize)
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

    def __hash__(self) -> int:
        return hash(self.id)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Schedule):
            return self.id == other.id

        return NotImplemented

    def __lt__(self, other: object) -> bool:
        if isinstance(other, Schedule):
            # Sort by next_fire_time first, exhausted schedules last
            if self.next_fire_time is not None and other.next_fire_time is not None:
                return self.next_fire_time < other.next_fire_time
            elif self.next_fire_time is None:
                return False
            elif other.next_fire_time is None:
                return True

            # In all other cases, sort by schedule ID
            return self.id < other.id

        return NotImplemented


@attrs.define(kw_only=True, frozen=True)
class ScheduleResult:
    """
    Represents a result of a schedule processing operation.

    :ivar schedule_id: ID of the schedule
    :ivar task_id: ID of the schedule's task
    :ivar trigger: the schedule's trigger
    :ivar last_fire_time: the schedule's trigger
    :ivar next_fire_time: the next
    """

    schedule_id: str
    task_id: str
    trigger: Trigger
    last_fire_time: datetime
    next_fire_time: datetime | None


@attrs.define(kw_only=True, order=False)
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
    :var metadata: key-value pairs for storing JSON compatible custom information
    :var ~datetime.datetime created_at: the time at which the job was created
    :var str | None acquired_by: the unique identifier of the scheduler that has
        acquired the job for execution
    :var str | None acquired_until: the time after which other schedulers are free to
        acquire the job for processing even if it is still marked as acquired
    """

    id: UUID = attrs.field(factory=uuid4, on_setattr=frozen)
    task_id: str = attrs.field(on_setattr=frozen)
    args: tuple = attrs.field(
        converter=tuple, default=(), repr=False, on_setattr=frozen
    )
    kwargs: dict[str, Any] = attrs.field(
        converter=dict, factory=dict, repr=False, on_setattr=frozen
    )
    schedule_id: str | None = attrs.field(default=None, on_setattr=frozen)
    scheduled_fire_time: datetime | None = attrs.field(
        converter=as_aware_datetime, default=None, on_setattr=frozen
    )
    executor: str = attrs.field(on_setattr=frozen)
    jitter: timedelta = attrs.field(
        converter=as_timedelta, factory=timedelta, repr=False, on_setattr=frozen
    )
    start_deadline: datetime | None = attrs.field(
        converter=as_aware_datetime, default=None, repr=False, on_setattr=frozen
    )
    result_expiration_time: timedelta = attrs.field(
        converter=as_timedelta, default=timedelta(), repr=False, on_setattr=frozen
    )
    metadata: MetadataType = attrs.field(validator=valid_metadata, factory=dict)
    created_at: datetime = attrs.field(
        converter=as_aware_datetime,
        factory=partial(datetime.now, timezone.utc),
        on_setattr=frozen,
    )
    acquired_by: str | None = attrs.field(default=None, repr=False)
    acquired_until: datetime | None = attrs.field(
        converter=as_aware_datetime, default=None, repr=False
    )

    @property
    def original_scheduled_time(self) -> datetime | None:
        """The scheduled time without any jitter included."""
        if self.scheduled_fire_time is None:
            return None

        return self.scheduled_fire_time - self.jitter

    def marshal(self, serializer: Serializer) -> dict[str, Any]:
        marshalled = attrs.asdict(self, recurse=False, value_serializer=serialize)
        marshalled["args"] = serializer.serialize(self.args)
        marshalled["kwargs"] = serializer.serialize(self.kwargs)
        if not self.acquired_by:
            del marshalled["acquired_by"]
            del marshalled["acquired_until"]

        return marshalled

    @classmethod
    def unmarshal(cls, serializer: Serializer, marshalled: dict[str, Any]) -> Job:
        if args := marshalled["args"]:
            marshalled["args"] = serializer.deserialize(args)

        if kwargs := marshalled["kwargs"]:
            marshalled["kwargs"] = serializer.deserialize(kwargs)

        return cls(**marshalled)

    def __hash__(self) -> int:
        return hash(self.id)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Job):
            return self.id == other.id

        return NotImplemented


@attrs.define(kw_only=True, frozen=True, eq=False)
class JobResult:
    """
    Represents the result of running a job.

    :var ~uuid.UUID job_id: the unique identifier of the job
    :var JobOutcome outcome: indicates how the job ended
    :var ~datetime.datetime started_at: the time when the job was submitted to the
        executor (``None`` if the job never started in the first place)
    :var ~datetime.datetime finished_at: the time when the job finished running, or was
        discarded during the job acquisition process
    :var ~datetime.datetime expires_at: the time when the result will expire
    :var BaseException | None exception: the exception object if the job ended due to an
        exception being raised
    :var return_value: the return value from the task function (if the job ran to
        completion successfully)
    """

    job_id: UUID
    outcome: JobOutcome = attrs.field(converter=as_enum(JobOutcome))
    started_at: datetime | None = attrs.field(converter=as_aware_datetime, default=None)
    finished_at: datetime = attrs.field(converter=as_aware_datetime)
    expires_at: datetime = attrs.field(converter=as_aware_datetime, repr=False)
    exception: BaseException | None = attrs.field(default=None, repr=False)
    return_value: Any = attrs.field(default=None, repr=False)

    @classmethod
    def from_job(
        cls,
        job: Job,
        outcome: JobOutcome,
        *,
        finished_at: datetime | None = None,
        started_at: datetime | None = None,
        exception: BaseException | None = None,
        return_value: Any = None,
    ) -> JobResult:
        real_finished_at = finished_at or datetime.now(timezone.utc)
        expires_at = real_finished_at + job.result_expiration_time
        return cls(
            job_id=job.id,
            outcome=outcome,
            started_at=started_at,
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

    def __hash__(self) -> int:
        return hash(self.job_id)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, JobResult):
            return self.job_id == other.job_id

        return NotImplemented
