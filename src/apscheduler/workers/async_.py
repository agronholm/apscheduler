from __future__ import annotations

import os
import platform
from asyncio import CancelledError
from contextlib import AsyncExitStack
from datetime import datetime, timezone
from inspect import isawaitable
from logging import Logger, getLogger
from types import TracebackType
from typing import Callable
from uuid import UUID

import anyio
import attrs
from anyio import (
    TASK_STATUS_IGNORED,
    create_task_group,
    get_cancelled_exc_class,
    move_on_after,
)
from anyio.abc import CancelScope, TaskGroup

from .._context import current_job, current_worker
from .._converters import as_async_datastore, as_async_eventbroker
from .._enums import JobOutcome, RunState
from .._events import JobAdded, JobReleased, WorkerStarted, WorkerStopped
from .._structures import Job, JobInfo, JobResult
from .._validators import positive_integer
from ..abc import AsyncDataStore, AsyncEventBroker
from ..eventbrokers.async_local import LocalAsyncEventBroker


@attrs.define(eq=False)
class AsyncWorker:
    """Runs jobs locally in a task group."""

    data_store: AsyncDataStore = attrs.field(converter=as_async_datastore)
    event_broker: AsyncEventBroker = attrs.field(
        converter=as_async_eventbroker, factory=LocalAsyncEventBroker
    )
    max_concurrent_jobs: int = attrs.field(
        kw_only=True, validator=positive_integer, default=100
    )
    identity: str = attrs.field(kw_only=True, default=None)
    logger: Logger | None = attrs.field(kw_only=True, default=getLogger(__name__))
    # True if a scheduler owns this worker
    _is_internal: bool = attrs.field(kw_only=True, default=False)

    _state: RunState = attrs.field(init=False, default=RunState.stopped)
    _wakeup_event: anyio.Event = attrs.field(init=False)
    _task_group: TaskGroup = attrs.field(init=False)
    _acquired_jobs: set[Job] = attrs.field(init=False, factory=set)
    _running_jobs: set[UUID] = attrs.field(init=False, factory=set)

    def __attrs_post_init__(self) -> None:
        if not self.identity:
            self.identity = f"{platform.node()}-{os.getpid()}-{id(self)}"

    async def __aenter__(self) -> AsyncWorker:
        self._task_group = create_task_group()
        await self._task_group.__aenter__()
        await self._task_group.start(self.run_until_stopped)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ) -> None:
        self._state = RunState.stopping
        self._wakeup_event.set()
        await self._task_group.__aexit__(exc_type, exc_val, exc_tb)
        del self._task_group
        del self._wakeup_event

    @property
    def state(self) -> RunState:
        """The current running state of the worker."""
        return self._state

    async def run_until_stopped(self, *, task_status=TASK_STATUS_IGNORED) -> None:
        """Run the worker until it is explicitly stopped."""
        if self._state is not RunState.stopped:
            raise RuntimeError(
                f'Cannot start the worker when it is in the "{self._state}" ' f"state"
            )

        self._state = RunState.starting
        self._wakeup_event = anyio.Event()
        async with AsyncExitStack() as exit_stack:
            if not self._is_internal:
                # Initialize the event broker
                await self.event_broker.start()
                exit_stack.push_async_exit(
                    lambda *exc_info: self.event_broker.stop(
                        force=exc_info[0] is not None
                    )
                )

                # Initialize the data store
                await self.data_store.start(self.event_broker)
                exit_stack.push_async_exit(
                    lambda *exc_info: self.data_store.stop(
                        force=exc_info[0] is not None
                    )
                )

            # Set the current worker
            token = current_worker.set(self)
            exit_stack.callback(current_worker.reset, token)

            # Wake up the worker if the data store emits a significant job event
            self.event_broker.subscribe(
                lambda event: self._wakeup_event.set(), {JobAdded}
            )

            # Signal that the worker has started
            self._state = RunState.started
            task_status.started()
            exception: BaseException | None = None
            try:
                await self.event_broker.publish_local(WorkerStarted())

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
                exception = exc
                raise
            finally:
                self._state = RunState.stopped

                # CancelledError is a subclass of Exception in Python 3.7
                if not exception or isinstance(exception, CancelledError):
                    self.logger.info("Worker stopped")
                elif isinstance(exception, Exception):
                    self.logger.exception("Worker crashed")
                elif exception:
                    self.logger.info(
                        f"Worker stopped due to {exception.__class__.__name__}"
                    )

                with move_on_after(3, shield=True):
                    await self.event_broker.publish_local(
                        WorkerStopped(exception=exception)
                    )

    async def stop(self, *, force: bool = False) -> None:
        """
        Signal the worker that it should stop running jobs.

        This method does not wait for the worker to actually stop.

        """
        if self._state in (RunState.starting, RunState.started):
            self._state = RunState.stopping
            event = anyio.Event()
            self.event_broker.subscribe(
                lambda ev: event.set(), {WorkerStopped}, one_shot=True
            )
            if force:
                self._task_group.cancel_scope.cancel()
            else:
                self._wakeup_event.set()

            await event.wait()

    async def _run_job(self, job: Job, func: Callable) -> None:
        try:
            # Check if the job started before the deadline
            start_time = datetime.now(timezone.utc)
            if job.start_deadline is not None and start_time > job.start_deadline:
                result = JobResult.from_job(
                    job,
                    outcome=JobOutcome.missed_start_deadline,
                    finished_at=start_time,
                )
                await self.data_store.release_job(self.identity, job.task_id, result)
                await self.event_broker.publish(
                    JobReleased.from_result(result, self.identity)
                )
                return

            token = current_job.set(JobInfo.from_job(job))
            try:
                retval = func(*job.args, **job.kwargs)
                if isawaitable(retval):
                    retval = await retval
            except get_cancelled_exc_class():
                self.logger.info("Job %s was cancelled", job.id)
                with CancelScope(shield=True):
                    result = JobResult.from_job(
                        job,
                        outcome=JobOutcome.cancelled,
                    )
                    await self.data_store.release_job(
                        self.identity, job.task_id, result
                    )
                    await self.event_broker.publish(
                        JobReleased.from_result(result, self.identity)
                    )
            except BaseException as exc:
                if isinstance(exc, Exception):
                    self.logger.exception("Job %s raised an exception", job.id)
                else:
                    self.logger.error(
                        "Job %s was aborted due to %s", job.id, exc.__class__.__name__
                    )

                result = JobResult.from_job(
                    job,
                    JobOutcome.error,
                    exception=exc,
                )
                await self.data_store.release_job(
                    self.identity,
                    job.task_id,
                    result,
                )
                await self.event_broker.publish(
                    JobReleased.from_result(result, self.identity)
                )
                if not isinstance(exc, Exception):
                    raise
            else:
                self.logger.info("Job %s completed successfully", job.id)
                result = JobResult.from_job(
                    job,
                    JobOutcome.success,
                    return_value=retval,
                )
                await self.data_store.release_job(self.identity, job.task_id, result)
                await self.event_broker.publish(
                    JobReleased.from_result(result, self.identity)
                )
            finally:
                current_job.reset(token)
        finally:
            self._running_jobs.remove(job.id)
