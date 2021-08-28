from __future__ import annotations

import os
import platform
from contextlib import AsyncExitStack
from datetime import datetime, timezone
from inspect import isawaitable
from logging import Logger, getLogger
from traceback import format_tb
from typing import Any, Callable, Iterable, Optional, Set, Type, Union
from uuid import UUID

import anyio
from anyio import TASK_STATUS_IGNORED, create_task_group, get_cancelled_exc_class
from anyio.abc import CancelScope, TaskGroup

from ..abc import AsyncDataStore, DataStore, EventSource, Job
from ..adapters import AsyncDataStoreAdapter
from ..enums import RunState
from ..events import (
    AsyncEventHub, Event, JobAdded, JobCompleted, JobDeadlineMissed, JobFailed, JobStarted,
    SubscriptionToken, WorkerStarted, WorkerStopped)


class AsyncWorker(EventSource):
    """Runs jobs locally in a task group."""

    _task_group: Optional[TaskGroup] = None
    _stop_event: Optional[anyio.Event] = None
    _state: RunState = RunState.stopped
    _acquire_cancel_scope: Optional[CancelScope] = None
    _datastore_subscription: SubscriptionToken
    _wakeup_event: anyio.Event

    def __init__(self, data_store: Union[DataStore, AsyncDataStore], *,
                 max_concurrent_jobs: int = 100, identity: Optional[str] = None,
                 logger: Optional[Logger] = None):
        self.max_concurrent_jobs = max_concurrent_jobs
        self.identity = identity or f'{platform.node()}-{os.getpid()}-{id(self)}'
        self.logger = logger or getLogger(__name__)
        self._acquired_jobs: Set[Job] = set()
        self._exit_stack = AsyncExitStack()
        self._events = AsyncEventHub()
        self._running_jobs: Set[UUID] = set()

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
        self._task_group = create_task_group()
        await self._exit_stack.enter_async_context(self._task_group)
        await self._task_group.start(self.run)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._state = RunState.stopping
        self._wakeup_event.set()
        await self._exit_stack.__aexit__(exc_type, exc_val, exc_tb)
        del self._task_group
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
            while self._state is RunState.started:
                limit = self.max_concurrent_jobs - len(self._running_jobs)
                with CancelScope() as self._acquire_cancel_scope:
                    try:
                        jobs = await self.data_store.acquire_jobs(self.identity, limit)
                    finally:
                        del self._acquire_cancel_scope

                for job in jobs:
                    self._running_jobs.add(job.id)
                    self._task_group.start_soon(self._run_job, job)

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

    # async def _run_job(self, job: Job) -> None:
    #     # Check if the job started before the deadline
    #     start_time = datetime.now(timezone.utc)
    #     if job.start_deadline is not None and start_time > job.start_deadline:
    #         event = JobDeadlineMissed(
    #             timestamp=datetime.now(timezone.utc), job_id=job.id, task_id=job.task_id,
    #             schedule_id=job.schedule_id, scheduled_fire_time=job.scheduled_fire_time,
    #             start_time=start_time, start_deadline=job.start_deadline)
    #         self._events.publish(event)
    #         return
    #
    #     now = datetime.now(timezone.utc)
    #     if job.start_deadline is not None:
    #         if now.timestamp() > job.start_deadline.timestamp():
    #             self.logger.info('Missed the deadline of job %r', job.id)
    #             event = JobDeadlineMissed(
    #                 now, job_id=job.id, task_id=job.task_id, schedule_id=job.schedule_id,
    #                 scheduled_fire_time=job.scheduled_fire_time, start_time=now,
    #                 start_deadline=job.start_deadline
    #             )
    #             await self.publish(event)
    #             return
    #
    #     # Set the job as running and publish a job update event
    #     self.logger.info('Started job %r', job.id)
    #     job.started_at = now
    #     event = JobUpdated(
    #         timestamp=now, job_id=job.id, task_id=job.task_id, schedule_id=job.schedule_id
    #     )
    #     await self.publish(event)
    #
    #     self._num_running_jobs += 1
    #     try:
    #         return_value = await self._call_job_func(job.func, job.args, job.kwargs)
    #     except BaseException as exc:
    #         self.logger.exception('Job %r raised an exception', job.id)
    #         event = JobFailed(
    #             timestamp=datetime.now(timezone.utc), job_id=job.id, task_id=job.task_id,
    #             schedule_id=job.schedule_id, scheduled_fire_time=job.scheduled_fire_time,
    #             start_time=now, start_deadline=job.start_deadline,
    #             traceback=format_exc(), exception=exc
    #         )
    #     else:
    #         self.logger.info('Job %r completed successfully', job.id)
    #         event = JobSuccessful(
    #             timestamp=datetime.now(timezone.utc), job_id=job.id, task_id=job.task_id,
    #             schedule_id=job.schedule_id, scheduled_fire_time=job.scheduled_fire_time,
    #             start_time=now, start_deadline=job.start_deadline, return_value=return_value
    #         )
    #
    #     self._num_running_jobs -= 1
    #     await self.data_store.release_jobs(self.identity, [job])
    #     await self.publish(event)
    #
    # async def _call_job_func(self, func: Callable, args: tuple, kwargs: Dict[str, Any]):
    #     if not self.run_sync_functions_in_event_loop and not iscoroutinefunction(func):
    #         wrapped = partial(func, *args, **kwargs)
    #         return await to_thread.run_sync(wrapped)
    #
    #     return_value = func(*args, **kwargs)
    #     if isinstance(return_value, Coroutine):
    #         return_value = await return_value
    #
    #     return return_value

    async def _run_job(self, job: Job) -> None:
        event: Event
        try:
            # Check if the job started before the deadline
            start_time = datetime.now(timezone.utc)
            if job.start_deadline is not None and start_time > job.start_deadline:
                event = JobDeadlineMissed(
                    timestamp=datetime.now(timezone.utc), job_id=job.id, task_id=job.task_id,
                    schedule_id=job.schedule_id, scheduled_fire_time=job.scheduled_fire_time,
                    start_time=start_time, start_deadline=job.start_deadline)
                self._events.publish(event)
                return

            event = JobStarted(
                timestamp=datetime.now(timezone.utc), job_id=job.id, task_id=job.task_id,
                schedule_id=job.schedule_id, scheduled_fire_time=job.scheduled_fire_time,
                start_time=start_time, start_deadline=job.start_deadline)
            self._events.publish(event)
            try:
                retval = job.func(*job.args, **job.kwargs)
                if isawaitable(retval):
                    retval = await retval
            except BaseException as exc:
                if exc.__class__.__module__ == 'builtins':
                    exc_name = exc.__class__.__qualname__
                else:
                    exc_name = f'{exc.__class__.__module__}.{exc.__class__.__qualname__}'

                formatted_traceback = '\n'.join(format_tb(exc.__traceback__))
                event = JobFailed(
                    timestamp=datetime.now(timezone.utc), job_id=job.id, task_id=job.task_id,
                    schedule_id=job.schedule_id, scheduled_fire_time=job.scheduled_fire_time,
                    start_time=start_time, start_deadline=job.start_deadline, exception=exc_name,
                    traceback=formatted_traceback)
                self._events.publish(event)
            else:
                event = JobCompleted(
                    timestamp=datetime.now(timezone.utc), job_id=job.id, task_id=job.task_id,
                    schedule_id=job.schedule_id, scheduled_fire_time=job.scheduled_fire_time,
                    start_time=start_time, start_deadline=job.start_deadline, return_value=retval)
                self._events.publish(event)
        finally:
            self._running_jobs.remove(job.id)
            await self.data_store.release_jobs(self.identity, [job])

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
