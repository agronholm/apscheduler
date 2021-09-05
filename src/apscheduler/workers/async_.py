from __future__ import annotations

import os
import platform
from contextlib import AsyncExitStack
from datetime import datetime, timezone
from inspect import isawaitable
from logging import Logger, getLogger
from typing import Any, Callable, Iterable, Optional, Type
from uuid import UUID

import anyio
from anyio import TASK_STATUS_IGNORED, create_task_group, get_cancelled_exc_class
from anyio.abc import CancelScope

from ..abc import AsyncDataStore, DataStore, EventSource, Job
from ..datastores.async_.sync_adapter import AsyncDataStoreAdapter
from ..enums import JobOutcome, RunState
from ..events import (
    AsyncEventHub, Event, JobAdded, JobCancelled, JobCompleted, JobDeadlineMissed, JobFailed,
    JobStarted, SubscriptionToken, WorkerStarted, WorkerStopped)
from ..structures import JobResult


class AsyncWorker(EventSource):
    """Runs jobs locally in a task group."""

    _stop_event: Optional[anyio.Event] = None
    _state: RunState = RunState.stopped
    _acquire_cancel_scope: Optional[CancelScope] = None
    _datastore_subscription: SubscriptionToken
    _wakeup_event: anyio.Event

    def __init__(self, data_store: DataStore | AsyncDataStore, *,
                 max_concurrent_jobs: int = 100, identity: Optional[str] = None,
                 logger: Optional[Logger] = None):
        self.max_concurrent_jobs = max_concurrent_jobs
        self.identity = identity or f'{platform.node()}-{os.getpid()}-{id(self)}'
        self.logger = logger or getLogger(__name__)
        self._acquired_jobs: set[Job] = set()
        self._exit_stack = AsyncExitStack()
        self._events = AsyncEventHub()
        self._running_jobs: set[UUID] = set()

        if self.max_concurrent_jobs < 1:
            raise ValueError('max_concurrent_jobs must be at least 1')

        if isinstance(data_store, DataStore):
            self.data_store = AsyncDataStoreAdapter(data_store)
        else:
            self.data_store = data_store

    @property
    def state(self) -> RunState:
        return self._state

    async def __aenter__(self):
        self._state = RunState.starting
        self._wakeup_event = anyio.Event()
        await self._exit_stack.__aenter__()
        await self._exit_stack.enter_async_context(self._events)

        # Initialize the data store
        await self._exit_stack.enter_async_context(self.data_store)
        relay_token = self._events.relay_events_from(self.data_store)
        self._exit_stack.callback(self.data_store.unsubscribe, relay_token)

        # Wake up the worker if the data store emits a significant job event
        wakeup_token = self.data_store.subscribe(
            lambda event: self._wakeup_event.set(), {JobAdded})
        self._exit_stack.callback(self.data_store.unsubscribe, wakeup_token)

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

    def subscribe(self, callback: Callable[[Event], Any],
                  event_types: Optional[Iterable[Type[Event]]] = None) -> SubscriptionToken:
        return self._events.subscribe(callback, event_types)

    def unsubscribe(self, token: SubscriptionToken) -> None:
        self._events.unsubscribe(token)

    async def run(self, *, task_status=TASK_STATUS_IGNORED) -> None:
        if self._state is not RunState.starting:
            raise RuntimeError(f'This function cannot be called while the worker is in the '
                               f'{self._state} state')

        # Signal that the worker has started
        self._state = RunState.started
        task_status.started()
        self._events.publish(WorkerStarted())

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
            self._state = RunState.stopped
            self._events.publish(WorkerStopped(exception=exc))
            raise

        self._state = RunState.stopped
        self._events.publish(WorkerStopped())

    async def _run_job(self, job: Job, func: Callable) -> None:
        try:
            # Check if the job started before the deadline
            start_time = datetime.now(timezone.utc)
            if job.start_deadline is not None and start_time > job.start_deadline:
                self._events.publish(JobDeadlineMissed.from_job(job, start_time))
                return

            self._events.publish(JobStarted.from_job(job, start_time))
            try:
                retval = func(*job.args, **job.kwargs)
                if isawaitable(retval):
                    retval = await retval
            except get_cancelled_exc_class():
                with CancelScope(shield=True):
                    result = JobResult(outcome=JobOutcome.cancelled)
                    await self.data_store.release_job(self.identity, job, result)

                self._events.publish(JobCancelled.from_job(job, start_time))
            except BaseException as exc:
                result = JobResult(outcome=JobOutcome.failure, exception=exc)
                await self.data_store.release_job(self.identity, job, result)
                self._events.publish(JobFailed.from_exception(job, start_time, exc))
                if not isinstance(exc, Exception):
                    raise
            else:
                result = JobResult(outcome=JobOutcome.success, return_value=retval)
                await self.data_store.release_job(self.identity, job, result)
                self._events.publish(JobCompleted.from_retval(job, start_time, retval))
        finally:
            self._running_jobs.remove(job.id)

    # async def stop(self, force: bool = False) -> None:
    #     self._running = False
    #     if self._acquire_cancel_scope:
    #         self._acquire_cancel_scope.cancel()
    #
    #     if force and self._task_group:
    #         self._task_group.cancel_scope.cancel()
    #
    # async def wait_until_stopped(self) -> None:
    #     if self._stop_event:
    #         await self._stop_event.wait()
