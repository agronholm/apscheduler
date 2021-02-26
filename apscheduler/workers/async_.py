import os
import platform
import threading
from asyncio import current_task, iscoroutinefunction
from collections.abc import Coroutine
from contextlib import AsyncExitStack
from datetime import datetime, timezone
from functools import partial
from logging import Logger, getLogger
from traceback import format_exc
from typing import Any, Callable, Dict, Optional, Set

from anyio import create_event, create_task_group, open_cancel_scope, run_sync_in_worker_thread
from anyio.abc import CancelScope, Event, TaskGroup

from ..abc import DataStore, Job
from ..events import EventHub, JobDeadlineMissed, JobFailed, JobSuccessful, JobUpdated


class AsyncWorker(EventHub):
    """Runs jobs locally in a task group."""

    _task_group: Optional[TaskGroup] = None
    _stop_event: Optional[Event] = None
    _running: bool = False
    _running_jobs: int = 0
    _acquire_cancel_scope: Optional[CancelScope] = None

    def __init__(self, data_store: DataStore, *, max_concurrent_jobs: int = 100,
                 identity: Optional[str] = None, logger: Optional[Logger] = None,
                 run_sync_functions_in_event_loop: bool = True):
        super().__init__()
        self.data_store = data_store
        self.max_concurrent_jobs = max_concurrent_jobs
        self.identity = identity or f'{platform.node()}-{os.getpid()}-{threading.get_ident()}'
        self.logger = logger or getLogger(__name__)
        self.run_sync_functions_in_event_loop = run_sync_functions_in_event_loop
        self._acquired_jobs: Set[Job] = set()
        self._exit_stack = AsyncExitStack()

        if self.max_concurrent_jobs < 1:
            raise ValueError('max_concurrent_jobs must be at least 1')

    async def __aenter__(self):
        await self._exit_stack.__aenter__()

        # Initialize the data store
        await self._exit_stack.enter_async_context(self.data_store)

        # Start the actual worker
        self._task_group = create_task_group()
        await self._exit_stack.enter_async_context(self._task_group)
        start_event = create_event()
        await self._task_group.spawn(self.run, start_event)
        await start_event.wait()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop(force=exc_type is not None)
        await self._exit_stack.__aexit__(exc_type, exc_val, exc_tb)

    async def run(self, start_event: Optional[Event] = None) -> None:
        self._stop_event = create_event()
        self._running = True
        if start_event:
            await start_event.set()

        while self._running:
            limit = self.max_concurrent_jobs - self._running_jobs
            jobs = []
            async with open_cancel_scope() as self._acquire_cancel_scope:
                try:
                    jobs = await self.data_store.acquire_jobs(self.identity, limit)
                finally:
                    del self._acquire_cancel_scope

            for job in jobs:
                await self._task_group.spawn(self._run_job, job)

        await self._stop_event.set()
        del self._stop_event
        del self._task_group

    async def _run_job(self, job: Job) -> None:
        # Check if the job started before the deadline
        now = datetime.now(timezone.utc)
        if job.start_deadline is not None:
            if now.timestamp() > job.start_deadline.timestamp():
                self.logger.info('Missed the deadline of job %r', job.id)
                event = JobDeadlineMissed(
                    now, job_id=job.id, task_id=job.task_id, schedule_id=job.schedule_id,
                    scheduled_fire_time=job.scheduled_fire_time, start_time=now,
                    start_deadline=job.start_deadline
                )
                await self.publish(event)
                return

        # Set the job as running and publish a job update event
        self.logger.info('Started job %r', job.id)
        job.started_at = now
        event = JobUpdated(
            timestamp=now, job_id=job.id, task_id=job.task_id, schedule_id=job.schedule_id
        )
        await self.publish(event)

        self._running_jobs += 1
        try:
            return_value = await self._call_job_func(job.func, job.args, job.kwargs)
        except BaseException as exc:
            self.logger.exception('Job %r raised an exception', job.id)
            event = JobFailed(
                timestamp=datetime.now(timezone.utc), job_id=job.id, task_id=job.task_id,
                schedule_id=job.schedule_id, scheduled_fire_time=job.scheduled_fire_time,
                start_time=now, start_deadline=job.start_deadline,
                traceback=format_exc(), exception=exc
            )
        else:
            self.logger.info('Job %r completed successfully', job.id)
            event = JobSuccessful(
                timestamp=datetime.now(timezone.utc), job_id=job.id, task_id=job.task_id,
                schedule_id=job.schedule_id, scheduled_fire_time=job.scheduled_fire_time,
                start_time=now, start_deadline=job.start_deadline, return_value=return_value
            )

        self._running_jobs -= 1
        await self.data_store.release_jobs(self.identity, [job])
        await self.publish(event)

    async def _call_job_func(self, func: Callable, args: tuple, kwargs: Dict[str, Any]):
        if not self.run_sync_functions_in_event_loop and not iscoroutinefunction(func):
            wrapped = partial(func, *args, **kwargs)
            return await run_sync_in_worker_thread(wrapped)

        return_value = func(*args, **kwargs)
        if isinstance(return_value, Coroutine):
            return_value = await return_value

        return return_value

    async def stop(self, force: bool = False) -> None:
        self._running = False
        if self._acquire_cancel_scope:
            await self._acquire_cancel_scope.cancel()

        if force and self._task_group:
            await self._task_group.cancel_scope.cancel()

    async def wait_until_stopped(self) -> None:
        if self._stop_event:
            await self._stop_event.wait()
