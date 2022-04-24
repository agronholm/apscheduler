from __future__ import annotations

from contextvars import ContextVar
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .schedulers.async_ import AsyncScheduler
    from .schedulers.sync import Scheduler
    from .structures import JobInfo
    from .workers.async_ import AsyncWorker
    from .workers.sync import Worker

#: The currently running (local) scheduler
current_scheduler: ContextVar[Scheduler | AsyncScheduler | None] = ContextVar(
    "current_scheduler", default=None
)
#: The worker running the current job
current_worker: ContextVar[Worker | AsyncWorker | None] = ContextVar(
    "current_worker", default=None
)
#: Metadata about the current job
job_info: ContextVar[JobInfo] = ContextVar("job_info")
