from __future__ import annotations

from datetime import datetime, timedelta, timezone
from functools import partial
from typing import Any, Callable
from uuid import UUID, uuid4

import attrs
import tenacity.stop
import tenacity.wait
from attrs.validators import instance_of

from . import abc
from ._converters import as_enum, as_timedelta
from ._enums import CoalescePolicy, JobOutcome
from .marshalling import callable_from_ref, callable_to_ref


def serialize(inst, field, value):
    if isinstance(value, frozenset):
        return list(value)

    return value


@attrs.define(kw_only=True)
class Task:
    id: str
    func: Callable = attrs.field(eq=False, order=False)
    max_running_jobs: int | None = attrs.field(eq=False, order=False, default=None)
    misfire_grace_time: timedelta | None = attrs.field(
        eq=False, order=False, default=None
    )
    state: Any = None

    def marshal(self, serializer: abc.Serializer) -> dict[str, Any]:
        marshalled = attrs.asdict(self, value_serializer=serialize)
        marshalled["func"] = callable_to_ref(self.func)
        marshalled["state"] = serializer.serialize(self.state) if self.state else None
        return marshalled

    @classmethod
    def unmarshal(cls, serializer: abc.Serializer, marshalled: dict[str, Any]) -> Task:
        marshalled["func"] = callable_from_ref(marshalled["func"])
        if marshalled["state"] is not None:
            marshalled["state"] = serializer.deserialize(marshalled["state"])

        return cls(**marshalled)


@attrs.define(kw_only=True)
class Schedule:
    id: str
    task_id: str = attrs.field(eq=False, order=False)
    trigger: abc.Trigger = attrs.field(eq=False, order=False)
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

    def marshal(self, serializer: abc.Serializer) -> dict[str, Any]:
        marshalled = attrs.asdict(self, value_serializer=serialize)
        marshalled["trigger"] = serializer.serialize(self.trigger)
        marshalled["args"] = serializer.serialize(self.args)
        marshalled["kwargs"] = serializer.serialize(self.kwargs)
        if not self.acquired_by:
            del marshalled["acquired_by"]
            del marshalled["acquired_until"]

        return marshalled

    @classmethod
    def unmarshal(
        cls, serializer: abc.Serializer, marshalled: dict[str, Any]
    ) -> Schedule:
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

    def marshal(self, serializer: abc.Serializer) -> dict[str, Any]:
        marshalled = attrs.asdict(self, value_serializer=serialize)
        marshalled["args"] = serializer.serialize(self.args)
        marshalled["kwargs"] = serializer.serialize(self.kwargs)
        if not self.acquired_by:
            del marshalled["acquired_by"]
            del marshalled["acquired_until"]

        return marshalled

    @classmethod
    def unmarshal(cls, serializer: abc.Serializer, marshalled: dict[str, Any]) -> Job:
        marshalled["args"] = serializer.deserialize(marshalled["args"])
        marshalled["kwargs"] = serializer.deserialize(marshalled["kwargs"])
        return cls(**marshalled)


@attrs.define(kw_only=True)
class JobInfo:
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
    job_id: UUID
    outcome: JobOutcome = attrs.field(
        eq=False, order=False, converter=as_enum(JobOutcome)
    )
    finished_at: datetime = attrs.field(
        eq=False, order=False, factory=partial(datetime.now, timezone.utc)
    )
    exception: BaseException | None = attrs.field(eq=False, order=False, default=None)
    return_value: Any = attrs.field(eq=False, order=False, default=None)

    def marshal(self, serializer: abc.Serializer) -> dict[str, Any]:
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
    def unmarshal(
        cls, serializer: abc.Serializer, marshalled: dict[str, Any]
    ) -> JobResult:
        if marshalled.get("exception"):
            marshalled["exception"] = serializer.deserialize(marshalled["exception"])
        elif marshalled.get("return_value"):
            marshalled["return_value"] = serializer.deserialize(
                marshalled["return_value"]
            )

        return cls(**marshalled)


@attrs.define(kw_only=True, frozen=True)
class RetrySettings:
    stop: tenacity.stop.stop_base = attrs.field(
        validator=instance_of(tenacity.stop.stop_base),
        default=tenacity.stop_after_delay(60),
    )
    wait: tenacity.wait.wait_base = attrs.field(
        validator=instance_of(tenacity.wait.wait_base),
        default=tenacity.wait_exponential(min=0.5, max=20),
    )

    @classmethod
    def fail_immediately(cls) -> RetrySettings:
        return cls(stop=tenacity.stop_after_attempt(1))
