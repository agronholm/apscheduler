from __future__ import annotations

import os
import platform
from contextlib import AsyncExitStack
from datetime import datetime, timezone
from inspect import isawaitable
from logging import Logger, getLogger
from typing import Callable, Optional
from uuid import UUID

import anyio
import attrs
from anyio import TASK_STATUS_IGNORED, create_task_group, get_cancelled_exc_class, move_on_after
from anyio.abc import CancelScope

from ..abc import AsyncDataStore, EventSource, Job
from ..context import current_worker, job_info
from ..converters import as_async_datastore
from ..enums import JobOutcome, RunState
from ..eventbrokers.async_local import LocalAsyncEventBroker
from ..events import JobAdded, WorkerStarted, WorkerStopped
from ..structures import JobInfo, JobResult
from ..validators import positive_integer


@attrs.define(eq=False)
class AsyncWorker:
    """Runs jobs locally in a task group."""
    data_store: AsyncDataStore = attrs.field(converter=as_async_datastore)
    max_concurrent_jobs: int = attrs.field(kw_only=True, validator=positive_integer, default=100)
    identity: str = attrs.field(kw_only=True, default=None)
    logger: Optional[Logger] = attrs.field(kw_only=True, default=getLogger(__name__))

    _state: RunState = attrs.field(init=False, default=RunState.stopped)
    _wakeup_event: anyio.Event = attrs.field(init=False, factory=anyio.Event)
    _acquired_jobs: set[Job] = attrs.field(init=False, factory=set)
    _events: LocalAsyncEventBroker = attrs.field(init=False, factory=LocalAsyncEventBroker)
    _running_jobs: set[UUID] = attrs.field(init=False, factory=set)
    _exit_stack: AsyncExitStack = attrs.field(init=False)

    def __attrs_post_init__(self) -> None:
        if not self.identity:
            self.identity = f'{platform.node()}-{os.getpid()}-{id(self)}'

    @property
    def events(self) -> EventSource:
        return self._events

    @property
    def state(self) -> RunState:
        return self._state

    async def __aenter__(self):
        self._state = RunState.starting
        self._wakeup_event = anyio.Event()
        self._exit_stack = AsyncExitStack()
        await self._exit_stack.__aenter__()
        await self._exit_stack.enter_async_context(self._events)

        # Initialize the data store and start relaying events to the worker's event broker
        await self._exit_stack.enter_async_context(self.data_store)
        self._exit_stack.enter_context(self.data_store.events.subscribe(self._events.publish))

        # Wake up the worker if the data store emits a significant job event
        self._exit_stack.enter_context(
            self.data_store.events.subscribe(lambda event: self._wakeup_event.set(), {JobAdded})
        )

        # Start the actual worker
        task_group = create_task_group()
        await self._exit_stack.enter_async_context(task_group)
        await task_group.start(self.run)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._state = RunState.stopping
        self._wakeup_event.set()
        await self._exit_stack.__aexit__(exc_type, exc_val, exc_tb)
        del self._wakeup_event

    async def run(self, *, task_status=TASK_STATUS_IGNORED) -> None:
        if self._state is not RunState.starting:
            raise RuntimeError(f'This function cannot be called while the worker is in the '
                               f'{self._state} state')

        # Set the current worker
        token = current_worker.set(self)

        # Signal that the worker has started
        self._state = RunState.started
        task_status.started()
        await self._events.publish(WorkerStarted())

        exception: Optional[BaseException] = None
        try:
            async with create_task_group() as tg:
                while self._state is RunState.started:
                    limit = self.max_concurrent_jobs - len(self._running_jobs)
                    jobs = await self.data_store.acquire_jobs(self.identity, limit)
                    for job in jobs:
                        task = await self.data_store.get_task(job.task_id)
                        self._running_jobs.add(job.id)
                        tg.start_soon(self._run_job, job, task.func)

                    await self._wakeup_event.wait()
                    self._wakeup_event = anyio.Event()
        except get_cancelled_exc_class():
            pass
        except BaseException as exc:
            self.logger.exception('Worker crashed')
            exception = exc
        else:
            self.logger.info('Worker stopped')
        finally:
            current_worker.reset(token)
            self._state = RunState.stopped
            self.logger.exception('Worker crashed')
            with move_on_after(3, shield=True):
                await self._events.publish(WorkerStopped(exception=exception))

    async def _run_job(self, job: Job, func: Callable) -> None:
        try:
            # Check if the job started before the deadline
            start_time = datetime.now(timezone.utc)
            if job.start_deadline is not None and start_time > job.start_deadline:
                result = JobResult(job_id=job.id, outcome=JobOutcome.missed_start_deadline)
                await self.data_store.release_job(self.identity, job.task_id, result)
                return

            token = job_info.set(JobInfo.from_job(job))
            try:
                retval = func(*job.args, **job.kwargs)
                if isawaitable(retval):
                    retval = await retval
            except get_cancelled_exc_class():
                with CancelScope(shield=True):
                    result = JobResult(job_id=job.id, outcome=JobOutcome.cancelled)
                    await self.data_store.release_job(self.identity, job.task_id, result)
            except BaseException as exc:
                result = JobResult(job_id=job.id, outcome=JobOutcome.error, exception=exc)
                await self.data_store.release_job(self.identity, job.task_id, result)
                if not isinstance(exc, Exception):
                    raise
            else:
                result = JobResult(job_id=job.id, outcome=JobOutcome.success, return_value=retval)
                await self.data_store.release_job(self.identity, job.task_id, result)
            finally:
                job_info.reset(token)
        finally:
            self._running_jobs.remove(job.id)
