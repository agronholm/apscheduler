from __future__ import annotations

from datetime import datetime, timedelta, timezone
from functools import partial
from typing import Any, Callable, Optional
from uuid import UUID, uuid4

import attr

from . import abc
from .converters import as_enum, as_timedelta
from .enums import CoalescePolicy, JobOutcome
from .marshalling import callable_from_ref, callable_to_ref


@attr.define(kw_only=True)
class Task:
    id: str
    func: Callable = attr.field(eq=False, order=False)
    max_running_jobs: Optional[int] = attr.field(eq=False, order=False, default=None)
    misfire_grace_time: Optional[timedelta] = attr.field(eq=False, order=False, default=None)
    state: Any = None

    def marshal(self, serializer: abc.Serializer) -> dict[str, Any]:
        marshalled = attr.asdict(self)
        marshalled['func'] = callable_to_ref(self.func)
        marshalled['state'] = serializer.serialize(self.state) if self.state else None
        return marshalled

    @classmethod
    def unmarshal(cls, serializer: abc.Serializer, marshalled: dict[str, Any]) -> Task:
        marshalled['func'] = callable_from_ref(marshalled['func'])
        if marshalled['state'] is not None:
            marshalled['state'] = serializer.deserialize(marshalled['state'])

        return cls(**marshalled)


@attr.define(kw_only=True)
class Schedule:
    id: str
    task_id: str = attr.field(eq=False, order=False)
    trigger: abc.Trigger = attr.field(eq=False, order=False)
    args: tuple = attr.field(eq=False, order=False, converter=tuple, default=())
    kwargs: dict[str, Any] = attr.field(eq=False, order=False, converter=dict, default=())
    coalesce: CoalescePolicy = attr.field(eq=False, order=False, default=CoalescePolicy.latest,
                                          converter=as_enum(CoalescePolicy))
    misfire_grace_time: Optional[timedelta] = attr.field(eq=False, order=False, default=None,
                                                         converter=as_timedelta)
    max_jitter: Optional[timedelta] = attr.field(eq=False, order=False, converter=as_timedelta,
                                                 default=None)
    tags: frozenset[str] = attr.field(eq=False, order=False, converter=frozenset, default=())
    next_fire_time: Optional[datetime] = attr.field(eq=False, order=False, default=None)
    last_fire_time: Optional[datetime] = attr.field(eq=False, order=False, default=None)
    acquired_by: Optional[str] = attr.field(eq=False, order=False, default=None)
    acquired_until: Optional[datetime] = attr.field(eq=False, order=False, default=None)

    def marshal(self, serializer: abc.Serializer) -> dict[str, Any]:
        marshalled = attr.asdict(self)
        marshalled['trigger'] = serializer.serialize(self.trigger)
        marshalled['args'] = serializer.serialize(self.args)
        marshalled['kwargs'] = serializer.serialize(self.kwargs)
        if not self.acquired_by:
            del marshalled['acquired_by']
            del marshalled['acquired_until']

        return marshalled

    @classmethod
    def unmarshal(cls, serializer: abc.Serializer, marshalled: dict[str, Any]) -> Schedule:
        marshalled['trigger'] = serializer.deserialize(marshalled['trigger'])
        marshalled['args'] = serializer.deserialize(marshalled['args'])
        marshalled['kwargs'] = serializer.deserialize(marshalled['kwargs'])
        return cls(**marshalled)

    @property
    def next_deadline(self) -> Optional[datetime]:
        if self.next_fire_time and self.misfire_grace_time:
            return self.next_fire_time + self.misfire_grace_time

        return None


@attr.define(kw_only=True)
class Job:
    id: UUID = attr.field(factory=uuid4)
    task_id: str = attr.field(eq=False, order=False)
    args: tuple = attr.field(eq=False, order=False, converter=tuple, default=())
    kwargs: dict[str, Any] = attr.field(eq=False, order=False, converter=dict, default=())
    schedule_id: Optional[str] = attr.field(eq=False, order=False, default=None)
    scheduled_fire_time: Optional[datetime] = attr.field(eq=False, order=False, default=None)
    jitter: timedelta = attr.field(eq=False, order=False, converter=as_timedelta,
                                   factory=timedelta)
    start_deadline: Optional[datetime] = attr.field(eq=False, order=False, default=None)
    tags: frozenset[str] = attr.field(eq=False, order=False, converter=frozenset, default=())
    created_at: datetime = attr.field(eq=False, order=False,
                                      factory=partial(datetime.now, timezone.utc))
    started_at: Optional[datetime] = attr.field(eq=False, order=False, default=None)
    acquired_by: Optional[str] = attr.field(eq=False, order=False, default=None)
    acquired_until: Optional[datetime] = attr.field(eq=False, order=False, default=None)

    @property
    def original_scheduled_time(self) -> Optional[datetime]:
        """The scheduled time without any jitter included."""
        if self.scheduled_fire_time is None:
            return None

        return self.scheduled_fire_time - self.jitter

    def marshal(self, serializer: abc.Serializer) -> dict[str, Any]:
        marshalled = attr.asdict(self)
        marshalled['args'] = serializer.serialize(self.args)
        marshalled['kwargs'] = serializer.serialize(self.kwargs)
        if not self.acquired_by:
            del marshalled['acquired_by']
            del marshalled['acquired_until']

        return marshalled

    @classmethod
    def unmarshal(cls, serializer: abc.Serializer, marshalled: dict[str, Any]) -> Job:
        marshalled['args'] = serializer.deserialize(marshalled['args'])
        marshalled['kwargs'] = serializer.deserialize(marshalled['kwargs'])
        return cls(**marshalled)


@attr.define(kw_only=True)
class JobInfo:
    job_id: UUID
    task_id: str
    schedule_id: Optional[str]
    scheduled_fire_time: Optional[datetime]
    jitter: timedelta
    start_deadline: Optional[datetime]
    tags: frozenset[str]

    @classmethod
    def from_job(cls, job: Job) -> JobInfo:
        return cls(job_id=job.id, task_id=job.task_id, schedule_id=job.schedule_id,
                   scheduled_fire_time=job.scheduled_fire_time, jitter=job.jitter,
                   start_deadline=job.start_deadline, tags=job.tags)


@attr.define(kw_only=True, frozen=True)
class JobResult:
    job_id: UUID
    outcome: JobOutcome = attr.field(eq=False, order=False, converter=as_enum(JobOutcome))
    finished_at: datetime = attr.field(eq=False, order=False,
                                       factory=partial(datetime.now, timezone.utc))
    exception: Optional[BaseException] = attr.field(eq=False, order=False, default=None)
    return_value: Any = attr.field(eq=False, order=False, default=None)

    def marshal(self, serializer: abc.Serializer) -> dict[str, Any]:
        marshalled = attr.asdict(self)
        if self.outcome is JobOutcome.error:
            marshalled['exception'] = serializer.serialize(self.exception)
        else:
            del marshalled['exception']

        if self.outcome is JobOutcome.success:
            marshalled['return_value'] = serializer.serialize(self.return_value)
        else:
            del marshalled['return_value']

        return marshalled

    @classmethod
    def unmarshal(cls, serializer: abc.Serializer, marshalled: dict[str, Any]) -> JobResult:
        if marshalled.get('exception'):
            marshalled['exception'] = serializer.deserialize(marshalled['exception'])
        elif marshalled.get('return_value'):
            marshalled['return_value'] = serializer.deserialize(marshalled['return_value'])

        return cls(**marshalled)
