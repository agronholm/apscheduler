from __future__ import annotations

__all__ = [
    "CoalescePolicy",
    "ConflictPolicy",
    "ConflictingIdError",
    "DeserializationError",
    "Job",
    "JobCancelled",
    "JobDeadlineMissed",
    "JobInfo",
    "JobLookupError",
    "JobOutcome",
    "JobResult",
    "JobResultNotReady",
    "MaxIterationsReached",
    "RetrySettings",
    "RunState",
    "Schedule",
    "ScheduleLookupError",
    "SerializationError",
    "Task",
    "TaskLookupError",
]

from typing import Any

from ._enums import CoalescePolicy, ConflictPolicy, JobOutcome, RunState
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
from ._structures import Job, JobInfo, JobResult, RetrySettings, Schedule, Task

# Re-export imports, so they look like they live directly in this package
value: Any
for value in list(locals().values()):
    if getattr(value, "__module__", "").startswith("apscheduler."):
        value.__module__ = __name__
