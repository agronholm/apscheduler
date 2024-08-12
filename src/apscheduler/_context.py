from __future__ import annotations

from contextvars import ContextVar
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._schedulers.async_ import AsyncScheduler
    from ._schedulers.sync import Scheduler
    from ._structures import Job

#: The currently running (local) scheduler
current_scheduler: ContextVar[Scheduler | None] = ContextVar(
    "current_scheduler", default=None
)
current_async_scheduler: ContextVar[AsyncScheduler | None] = ContextVar(
    "current_async_scheduler", default=None
)
#: Metadata about the current job
current_job: ContextVar[Job] = ContextVar("job_info")
