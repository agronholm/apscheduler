from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Callable, Dict, FrozenSet, Optional
from uuid import UUID, uuid4

import attr

from . import abc
from .enums import JobOutcome
from .policies import CoalescePolicy


@attr.define(kw_only=True)
class Task:
    id: str
    func: Callable = attr.field(eq=False, order=False)
    max_instances: Optional[int] = attr.field(eq=False, order=False, default=None)
    metadata_arg: Optional[str] = attr.field(eq=False, order=False, default=None)
    stateful: bool = attr.field(eq=False, order=False, default=False)
    misfire_grace_time: Optional[timedelta] = attr.field(eq=False, order=False, default=None)


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

    @property
    def next_deadline(self) -> Optional[datetime]:
        if self.next_fire_time and self.misfire_grace_time:
            return self.next_fire_time + self.misfire_grace_time

        return None


@attr.define(kw_only=True)
class Job:
    id: UUID = attr.field(factory=uuid4)
    task_id: str = attr.field(eq=False, order=False)
    func: Callable = attr.field(eq=False, order=False)
    args: tuple = attr.field(eq=False, order=False, default=())
    kwargs: Dict[str, Any] = attr.field(eq=False, order=False, factory=dict)
    schedule_id: Optional[str] = attr.field(eq=False, order=False, default=None)
    scheduled_fire_time: Optional[datetime] = attr.field(eq=False, order=False, default=None)
    start_deadline: Optional[datetime] = attr.field(eq=False, order=False, default=None)
    tags: FrozenSet[str] = attr.field(eq=False, order=False, factory=frozenset)
    started_at: Optional[datetime] = attr.field(eq=False, order=False, init=False, default=None)


@attr.define(eq=False, order=False, frozen=True)
class JobResult:
    outcome: JobOutcome
    exception: Optional[BaseException] = None
    return_value: Any = None
