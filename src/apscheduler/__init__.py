from __future__ import annotations

__all__ = [
    "CoalescePolicy",
    "ConflictPolicy",
    "ConflictingIdError",
    "DeserializationError",
    "JobCancelled",
    "JobDeadlineMissed",
    "JobLookupError",
    "JobOutcome",
    "JobResultNotReady",
    "MaxIterationsReached",
    "RunState",
    "ScheduleLookupError",
    "SerializationError",
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

# Re-export imports, so they look like they live directly in this package
value: Any
for value in list(locals().values()):
    if getattr(value, "__module__", "").startswith("apscheduler."):
        value.__module__ = __name__
