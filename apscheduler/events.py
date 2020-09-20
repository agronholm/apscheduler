from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional, Set


@dataclass(frozen=True)
class Event:
    timestamp: datetime


@dataclass(frozen=True)
class ExecutorEvent(Event):
    job_id: str
    task_id: str
    schedule_id: Optional[str]
    scheduled_start_time: Optional[datetime]


@dataclass(frozen=True)
class JobSubmissionFailed(ExecutorEvent):
    exception: BaseException
    formatted_traceback: str


@dataclass(frozen=True)
class JobAdded(ExecutorEvent):
    pass


@dataclass(frozen=True)
class JobUpdated(ExecutorEvent):
    pass


@dataclass(frozen=True)
class JobDeadlineMissed(ExecutorEvent):
    start_deadline: datetime


@dataclass(frozen=True)
class JobSuccessful(ExecutorEvent):
    start_time: datetime
    start_deadline: Optional[datetime]
    return_value: Any


@dataclass(frozen=True)
class JobFailed(ExecutorEvent):
    start_time: datetime
    start_deadline: Optional[datetime]
    formatted_traceback: str
    exception: Optional[BaseException] = None


@dataclass(frozen=True)
class DataStoreEvent(Event):
    schedule_ids: Set[str]


@dataclass(frozen=True)
class SchedulesAdded(DataStoreEvent):
    earliest_next_fire_time: Optional[datetime]


@dataclass(frozen=True)
class SchedulesUpdated(DataStoreEvent):
    earliest_next_fire_time: Optional[datetime]


@dataclass(frozen=True)
class SchedulesRemoved(DataStoreEvent):
    pass
