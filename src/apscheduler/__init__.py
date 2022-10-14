from __future__ import annotations

__all__ = [
    "CoalescePolicy",
    "ConflictPolicy",
    "ConflictingIdError",
    "DataStoreEvent",
    "DeserializationError",
    "Event",
    "Job",
    "JobAcquired",
    "JobAdded",
    "JobCancelled",
    "JobDeadlineMissed",
    "JobDeserializationFailed",
    "JobInfo",
    "JobLookupError",
    "JobOutcome",
    "JobReleased",
    "JobRemoved",
    "JobResult",
    "JobResultNotReady",
    "MaxIterationsReached",
    "RetrySettings",
    "RunState",
    "Schedule",
    "ScheduleLookupError",
    "SerializationError",
    "ScheduleAdded",
    "ScheduleUpdated",
    "ScheduleRemoved",
    "ScheduleDeserializationFailed",
    "SchedulerEvent",
    "SchedulerRole",
    "SchedulerStarted",
    "SchedulerStopped",
    "Task",
    "TaskAdded",
    "TaskLookupError",
    "TaskUpdated",
    "TaskRemoved",
    "current_async_scheduler",
    "current_scheduler",
    "current_job",
]

from typing import Any

from ._context import current_async_scheduler, current_job, current_scheduler
from ._enums import CoalescePolicy, ConflictPolicy, JobOutcome, RunState, SchedulerRole
from ._events import (
    DataStoreEvent,
    Event,
    JobAcquired,
    JobAdded,
    JobDeserializationFailed,
    JobReleased,
    JobRemoved,
    ScheduleAdded,
    ScheduleDeserializationFailed,
    ScheduleRemoved,
    SchedulerEvent,
    SchedulerStarted,
    SchedulerStopped,
    ScheduleUpdated,
    TaskAdded,
    TaskRemoved,
    TaskUpdated,
)
from ._exceptions import (
    ConflictingIdError,
    DeserializationError,
    JobCancelled,
    JobDeadlineMissed,
    JobLookupError,
    JobResultNotReady,
    MaxIterationsReached,
    ScheduleLookupError,
    SerializationError,
    TaskLookupError,
)
from ._retry import RetrySettings
from ._structures import Job, JobInfo, JobResult, Schedule, Task

# Re-export imports, so they look like they live directly in this package
value: Any
for value in list(locals().values()):
    if getattr(value, "__module__", "").startswith("apscheduler."):
        value.__module__ = __name__
