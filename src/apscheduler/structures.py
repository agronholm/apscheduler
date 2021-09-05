from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Callable, Dict, FrozenSet, Optional
from uuid import UUID, uuid4

import attr

from . import abc
from .enums import CoalescePolicy, JobOutcome
from .marshalling import callable_from_ref, callable_to_ref


@attr.define(kw_only=True)
class Task:
    id: str
    func: Callable = attr.field(eq=False, order=False)
    max_running_jobs: Optional[int] = attr.field(eq=False, order=False, default=None)
    state: Any = None
    misfire_grace_time: Optional[timedelta] = attr.field(eq=False, order=False, default=None)

    def marshal(self, serializer: abc.Serializer) -> Dict[str, Any]:
        marshalled = attr.asdict(self)
        marshalled['func'] = callable_to_ref(self.func)
        marshalled['state'] = serializer.serialize(self.state) if self.state else None
        return marshalled

    @classmethod
    def unmarshal(cls, serializer: abc.Serializer, marshalled: Dict[str, Any]) -> Task:
        marshalled['func'] = callable_from_ref(marshalled['func'])
        if marshalled['state'] is not None:
            marshalled['state'] = serializer.deserialize(marshalled['state'])

        return cls(**marshalled)


@attr.define(kw_only=True)
class Schedule:
    id: str
    task_id: str = attr.field(eq=False, order=False)
    trigger: abc.Trigger = attr.field(eq=False, order=False)
    args: tuple = attr.field(eq=False, order=False, default=())
    kwargs: Dict[str, Any] = attr.field(eq=False, order=False, factory=dict)
    coalesce: CoalescePolicy = attr.field(eq=False, order=False, default=CoalescePolicy.latest)
    misfire_grace_time: Optional[timedelta] = attr.field(eq=False, order=False, default=None)
    tags: FrozenSet[str] = attr.field(eq=False, order=False, factory=frozenset)
    next_fire_time: Optional[datetime] = attr.field(eq=False, order=False, init=False,
                                                    default=None)
    last_fire_time: Optional[datetime] = attr.field(eq=False, order=False, init=False,
                                                    default=None)

    def marshal(self, serializer: abc.Serializer) -> Dict[str, Any]:
        marshalled = attr.asdict(self)
        marshalled['trigger_type'] = serializer.serialize(self.args)
        marshalled['trigger_data'] = serializer.serialize(self.trigger)
        marshalled['args'] = serializer.serialize(self.args) if self.args else None
        marshalled['kwargs'] = serializer.serialize(self.kwargs) if self.kwargs else None
        marshalled['tags'] = list(self.tags)
        return marshalled

    @property
    def next_deadline(self) -> Optional[datetime]:
        if self.next_fire_time and self.misfire_grace_time:
            return self.next_fire_time + self.misfire_grace_time

        return None


@attr.define(kw_only=True)
class Job:
    id: UUID = attr.field(factory=uuid4)
    task_id: str = attr.field(eq=False, order=False)
    args: tuple = attr.field(eq=False, order=False, default=())
    kwargs: Dict[str, Any] = attr.field(eq=False, order=False, factory=dict)
    schedule_id: Optional[str] = attr.field(eq=False, order=False, default=None)
    scheduled_fire_time: Optional[datetime] = attr.field(eq=False, order=False, default=None)
    start_deadline: Optional[datetime] = attr.field(eq=False, order=False, default=None)
    tags: FrozenSet[str] = attr.field(eq=False, order=False, factory=frozenset)
    started_at: Optional[datetime] = attr.field(eq=False, order=False, init=False, default=None)

    def marshal(self, serializer: abc.Serializer) -> Dict[str, Any]:
        marshalled = attr.asdict(self)
        marshalled['args'] = serializer.serialize(self.args) if self.args else None
        marshalled['kwargs'] = serializer.serialize(self.kwargs) if self.kwargs else None
        marshalled['tags'] = list(self.tags)
        return marshalled

    @classmethod
    def unmarshal(cls, serializer: abc.Serializer, marshalled: Dict[str, Any]) -> Task:
        for key in ('args', 'kwargs'):
            if marshalled[key] is not None:
                marshalled[key] = serializer.deserialize(marshalled[key])

        marshalled['tags'] = frozenset(marshalled['tags'])
        return cls(**marshalled)


@attr.define(eq=False, order=False, frozen=True)
class JobResult:
    outcome: JobOutcome
    exception: Optional[BaseException] = None
    return_value: Any = None
