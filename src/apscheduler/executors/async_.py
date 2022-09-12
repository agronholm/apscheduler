from __future__ import annotations

from collections.abc import Callable
from inspect import isawaitable
from typing import Any

from .._structures import Job
from ..abc import JobExecutor


class AsyncJobExecutor(JobExecutor):
    """
    Executes functions directly on the event loop thread.

    If the function returns a coroutine object (or another kind of awaitable), that is
    awaited on and its return value is used as the job's return value.
    """

    async def run_job(self, func: Callable[..., Any], job: Job) -> Any:
        retval = func(*job.args, **job.kwargs)
        if isawaitable(retval):
            retval = await retval

        return retval
