from __future__ import annotations

from typing import Any

from ._context import current_async_scheduler as current_async_scheduler
from ._context import current_job as current_job
from ._context import current_scheduler as current_scheduler
from ._decorators import task as task
from ._enums import CoalescePolicy as CoalescePolicy
from ._enums import ConflictPolicy as ConflictPolicy
from ._enums import JobOutcome as JobOutcome
from ._enums import RunState as RunState
from ._enums import SchedulerRole as SchedulerRole
from ._events import DataStoreEvent as DataStoreEvent
from ._events import Event as Event
from ._events import JobAcquired as JobAcquired
from ._events import JobAdded as JobAdded
from ._events import JobDeserializationFailed as JobDeserializationFailed
from ._events import JobReleased as JobReleased
from ._events import JobRemoved as JobRemoved
from ._events import ScheduleAdded as ScheduleAdded
from ._events import ScheduleDeserializationFailed as ScheduleDeserializationFailed
from ._events import ScheduleRemoved as ScheduleRemoved
from ._events import SchedulerEvent as SchedulerEvent
from ._events import SchedulerStarted as SchedulerStarted
from ._events import SchedulerStopped as SchedulerStopped
from ._events import ScheduleUpdated as ScheduleUpdated
from ._events import TaskAdded as TaskAdded
from ._events import TaskRemoved as TaskRemoved
from ._events import TaskUpdated as TaskUpdated
from ._exceptions import CallableLookupError as CallableLookupError
from ._exceptions import ConflictingIdError as ConflictingIdError
from ._exceptions import DeserializationError as DeserializationError
from ._exceptions import JobCancelled as JobCancelled
from ._exceptions import JobDeadlineMissed as JobDeadlineMissed
from ._exceptions import JobLookupError as JobLookupError
from ._exceptions import JobResultNotReady as JobResultNotReady
from ._exceptions import MaxIterationsReached as MaxIterationsReached
from ._exceptions import ScheduleLookupError as ScheduleLookupError
from ._exceptions import SerializationError as SerializationError
from ._exceptions import TaskLookupError as TaskLookupError
from ._retry import RetryMixin as RetryMixin
from ._retry import RetrySettings as RetrySettings
from ._schedulers.async_ import AsyncScheduler as AsyncScheduler
from ._schedulers.sync import Scheduler as Scheduler
from ._structures import Job as Job
from ._structures import JobResult as JobResult
from ._structures import Schedule as Schedule
from ._structures import ScheduleResult as ScheduleResult
from ._structures import Task as Task
from ._structures import TaskDefaults as TaskDefaults
from ._utils import UnsetValue as UnsetValue

# Re-export imports, so they look like they live directly in this package
value: Any
for value in list(locals().values()):
    if getattr(value, "__module__", "").startswith("apscheduler."):
        value.__module__ = __name__
