from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, FrozenSet, Optional
from uuid import UUID, uuid4

import attr

from . import abc
from .enums import JobOutcome
from .policies import CoalescePolicy


@dataclass(eq=False)
class Task:
    id: str
    func: Callable
    max_instances: Optional[int] = None
    metadata_arg: Optional[str] = None
    stateful: bool = False
    misfire_grace_time: Optional[timedelta] = None

    def __eq__(self, other) -> bool:
        if isinstance(other, Task):
            return self.id == other.id

        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.id)


@dataclass(eq=False)
class Schedule:
    id: str
    task_id: str
    trigger: abc.Trigger
    args: tuple
    kwargs: Dict[str, Any]
    coalesce: CoalescePolicy
    misfire_grace_time: Optional[timedelta]
    tags: FrozenSet[str]
    next_fire_time: Optional[datetime] = None
    last_fire_time: Optional[datetime] = field(init=False, default=None)

    @property
    def next_deadline(self) -> Optional[datetime]:
        if self.next_fire_time and self.misfire_grace_time:
            return self.next_fire_time + self.misfire_grace_time

        return None

    def __eq__(self, other) -> bool:
        if isinstance(other, Schedule):
            return self.id == other.id

        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.id)


@dataclass(eq=False)
class Job:
    id: UUID = field(init=False, default_factory=uuid4)
    task_id: str
    func: Callable
    args: tuple
    kwargs: Dict[str, Any]
    schedule_id: Optional[str] = None
    scheduled_fire_time: Optional[datetime] = None
    start_deadline: Optional[datetime] = None
    tags: FrozenSet[str] = field(default_factory=frozenset)
    started_at: Optional[datetime] = field(init=False, default=None)

    def __eq__(self, other) -> bool:
        if isinstance(other, Job):
            return self.id == other.id

        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.id)


@attr.define(frozen=True)
class JobResult:
    outcome: JobOutcome
    exception: Optional[BaseException] = None
    return_value: Any = None
