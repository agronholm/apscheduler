from __future__ import annotations

import os
import platform
import random
from contextlib import AsyncExitStack
from datetime import datetime, timedelta, timezone
from logging import Logger, getLogger
from typing import Any, Callable, Iterable, Mapping
from uuid import UUID, uuid4

import anyio
import attrs
from anyio import TASK_STATUS_IGNORED, create_task_group, get_cancelled_exc_class, move_on_after

from ..abc import AsyncDataStore, EventSource, Job, Schedule, Trigger
from ..context import current_scheduler
from ..converters import as_async_datastore
from ..datastores.memory import MemoryDataStore
from ..enums import CoalescePolicy, ConflictPolicy, JobOutcome, RunState
from ..eventbrokers.async_local import LocalAsyncEventBroker
from ..events import (
    Event, JobReleased, ScheduleAdded, SchedulerStarted, SchedulerStopped, ScheduleUpdated)
from ..exceptions import JobCancelled, JobDeadlineMissed, JobLookupError
from ..marshalling import callable_to_ref
from ..structures import JobResult, Task
from ..workers.async_ import AsyncWorker

_microsecond_delta = timedelta(microseconds=1)
_zero_timedelta = timedelta()


@attrs.define(eq=False)
class AsyncScheduler:
    """An asynchronous (AnyIO based) scheduler implementation."""

    data_store: AsyncDataStore = attrs.field(converter=as_async_datastore, factory=MemoryDataStore)
    identity: str = attrs.field(kw_only=True, default=None)
    start_worker: bool = attrs.field(kw_only=True, default=True)
    logger: Logger | None = attrs.field(kw_only=True, default=getLogger(__name__))

    _state: RunState = attrs.field(init=False, default=RunState.stopped)
    _wakeup_event: anyio.Event = attrs.field(init=False)
    _worker: AsyncWorker | None = attrs.field(init=False, default=None)
    _events: LocalAsyncEventBroker = attrs.field(init=False, factory=LocalAsyncEventBroker)
    _exit_stack: AsyncExitStack = attrs.field(init=False)

    def __attrs_post_init__(self) -> None:
        if not self.identity:
            self.identity = f'{platform.node()}-{os.getpid()}-{id(self)}'

    @property
    def events(self) -> EventSource:
        return self._events

    @property
    def worker(self) -> AsyncWorker | None:
        return self._worker

    async def __aenter__(self):
        self._state = RunState.starting
        self._wakeup_event = anyio.Event()
        self._exit_stack = AsyncExitStack()
        await self._exit_stack.__aenter__()
        await self._exit_stack.enter_async_context(self._events)

        # Initialize the data store and start relaying events to the scheduler's event broker
        await self._exit_stack.enter_async_context(self.data_store)
        self._exit_stack.enter_context(self.data_store.events.subscribe(self._events.publish))

        # Wake up the scheduler if the data store emits a significant schedule event
        self._exit_stack.enter_context(
            self.data_store.events.subscribe(
                self._schedule_added_or_modified, {ScheduleAdded, ScheduleUpdated}
            )
        )

        # Start the built-in worker, if configured to do so
        if self.start_worker:
            token = current_scheduler.set(self)
            try:
                self._worker = AsyncWorker(self.data_store)
                await self._exit_stack.enter_async_context(self._worker)
            finally:
                current_scheduler.reset(token)

        # Start the worker and return when it has signalled readiness or raised an exception
        task_group = create_task_group()
        await self._exit_stack.enter_async_context(task_group)
        await task_group.start(self.run)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._state = RunState.stopping
        self._wakeup_event.set()
        await self._exit_stack.__aexit__(exc_type, exc_val, exc_tb)
        self._state = RunState.stopped
        del self._wakeup_event

    def _schedule_added_or_modified(self, event: Event) -> None:
        self.logger.debug('Detected a %s event – waking up the scheduler', type(event).__name__)
        self._wakeup_event.set()

    async def add_schedule(
        self, func_or_task_id: str | Callable, trigger: Trigger, *, id: str | None = None,
        args: Iterable | None = None, kwargs: Mapping[str, Any] | None = None,
        coalesce: CoalescePolicy = CoalescePolicy.latest,
        misfire_grace_time: float | timedelta | None = None,
        max_jitter: float | timedelta | None = None, tags: Iterable[str] | None = None,
        conflict_policy: ConflictPolicy = ConflictPolicy.do_nothing
    ) -> str:
        id = id or str(uuid4())
        args = tuple(args or ())
        kwargs = dict(kwargs or {})
        tags = frozenset(tags or ())
        if isinstance(misfire_grace_time, (int, float)):
            misfire_grace_time = timedelta(seconds=misfire_grace_time)

        if callable(func_or_task_id):
            task = Task(id=callable_to_ref(func_or_task_id), func=func_or_task_id)
            await self.data_store.add_task(task)
        else:
            task = await self.data_store.get_task(func_or_task_id)

        schedule = Schedule(id=id, task_id=task.id, trigger=trigger, args=args, kwargs=kwargs,
                            coalesce=coalesce, misfire_grace_time=misfire_grace_time,
                            max_jitter=max_jitter, tags=tags)
        schedule.next_fire_time = trigger.next()
        await self.data_store.add_schedule(schedule, conflict_policy)
        self.logger.info('Added new schedule (task=%r, trigger=%r); next run time at %s', task,
                         trigger, schedule.next_fire_time)
        return schedule.id

    async def get_schedule(self, id: str) -> Schedule:
        schedules = await self.data_store.get_schedules({id})
        return schedules[0]

    async def remove_schedule(self, schedule_id: str) -> None:
        await self.data_store.remove_schedules({schedule_id})

    async def add_job(
        self, func_or_task_id: str | Callable, *, args: Iterable | None = None,
        kwargs: Mapping[str, Any] | None = None, tags: Iterable[str] | None = None
    ) -> UUID:
        """
        Add a job to the data store.

        :param func_or_task_id:
        :param args: positional arguments to call the target callable with
        :param kwargs: keyword arguments to call the target callable with
        :param tags:
        :return: the ID of the newly created job

        """
        if callable(func_or_task_id):
            task = Task(id=callable_to_ref(func_or_task_id), func=func_or_task_id)
            await self.data_store.add_task(task)
        else:
            task = await self.data_store.get_task(func_or_task_id)

        job = Job(task_id=task.id, args=args or (), kwargs=kwargs or {}, tags=tags or frozenset())
        await self.data_store.add_job(job)
        return job.id

    async def get_job_result(self, job_id: UUID, *, wait: bool = True) -> JobResult:
        """
        Retrieve the result of a job.

        :param job_id: the ID of the job
        :param wait: if ``True``, wait until the job has ended (one way or another), ``False`` to
                     raise an exception if the result is not yet available
        :raises JobLookupError: if the job does not exist in the data store

        """
        wait_event = anyio.Event()

        def listener(event: JobReleased) -> None:
            if event.job_id == job_id:
                wait_event.set()

        with self.data_store.events.subscribe(listener, {JobReleased}):
            result = await self.data_store.get_job_result(job_id)
            if result:
                return result
            elif not wait:
                raise JobLookupError(job_id)

            await wait_event.wait()

        result = await self.data_store.get_job_result(job_id)
        assert isinstance(result, JobResult)
        return result

    async def run_job(
        self, func_or_task_id: str | Callable, *, args: Iterable | None = None,
        kwargs: Mapping[str, Any] | None = None, tags: Iterable[str] | None = ()
    ) -> Any:
        """
        Convenience method to add a job and then return its result (or raise its exception).

        :returns: the return value of the target function

        """
        job_complete_event = anyio.Event()

        def listener(event: JobReleased) -> None:
            if event.job_id == job_id:
                job_complete_event.set()

        job_id: UUID | None = None
        with self.data_store.events.subscribe(listener, {JobReleased}):
            job_id = await self.add_job(func_or_task_id, args=args, kwargs=kwargs, tags=tags)
            await job_complete_event.wait()

        result = await self.get_job_result(job_id)
        if result.outcome is JobOutcome.success:
            return result.return_value
        elif result.outcome is JobOutcome.error:
            raise result.exception
        elif result.outcome is JobOutcome.missed_start_deadline:
            raise JobDeadlineMissed
        elif result.outcome is JobOutcome.cancelled:
            raise JobCancelled
        else:
            raise RuntimeError(f'Unknown job outcome: {result.outcome}')

    async def run(self, *, task_status=TASK_STATUS_IGNORED) -> None:
        if self._state is not RunState.starting:
            raise RuntimeError(f'This function cannot be called while the scheduler is in the '
                               f'{self._state} state')

        # Signal that the scheduler has started
        self._state = RunState.started
        task_status.started()
        await self._events.publish(SchedulerStarted())

        exception: BaseException | None = None
        try:
            while self._state is RunState.started:
                schedules = await self.data_store.acquire_schedules(self.identity, 100)
                now = datetime.now(timezone.utc)
                for schedule in schedules:
                    # Calculate a next fire time for the schedule, if possible
                    fire_times = [schedule.next_fire_time]
                    calculate_next = schedule.trigger.next
                    while True:
                        try:
                            fire_time = calculate_next()
                        except Exception:
                            self.logger.exception(
                                'Error computing next fire time for schedule %r of task %r – '
                                'removing schedule', schedule.id, schedule.task_id)
                            break

                        # Stop if the calculated fire time is in the future
                        if fire_time is None or fire_time > now:
                            schedule.next_fire_time = fire_time
                            break

                        # Only keep all the fire times if coalesce policy = "all"
                        if schedule.coalesce is CoalescePolicy.all:
                            fire_times.append(fire_time)
                        elif schedule.coalesce is CoalescePolicy.latest:
                            fire_times[0] = fire_time

                    # Add one or more jobs to the job queue
                    max_jitter = schedule.max_jitter.total_seconds() if schedule.max_jitter else 0
                    for i, fire_time in enumerate(fire_times):
                        # Calculate a jitter if max_jitter > 0
                        jitter = _zero_timedelta
                        if max_jitter:
                            if i + 1 < len(fire_times):
                                next_fire_time = fire_times[i + 1]
                            else:
                                next_fire_time = schedule.next_fire_time

                            if next_fire_time is not None:
                                # Jitter must never be so high that it would cause a fire time to
                                # equal or exceed the next fire time
                                jitter_s = min([
                                    max_jitter,
                                    (next_fire_time - fire_time
                                     - _microsecond_delta).total_seconds()
                                ])
                                jitter = timedelta(seconds=random.uniform(0, jitter_s))
                                fire_time += jitter

                        schedule.last_fire_time = fire_time
                        job = Job(task_id=schedule.task_id, args=schedule.args,
                                  kwargs=schedule.kwargs, schedule_id=schedule.id,
                                  scheduled_fire_time=fire_time, jitter=jitter,
                                  start_deadline=schedule.next_deadline, tags=schedule.tags)
                        await self.data_store.add_job(job)

                    # Update the schedules (and release the scheduler's claim on them)
                    await self.data_store.release_schedules(self.identity, schedules)

                # If we received fewer schedules than the maximum amount, sleep until the next
                # schedule is due or the scheduler is explicitly woken up
                wait_time = None
                if len(schedules) < 100:
                    next_fire_time = await self.data_store.get_next_schedule_run_time()
                    if next_fire_time:
                        wait_time = (next_fire_time - datetime.now(timezone.utc)).total_seconds()
                        self.logger.debug('Sleeping %.3f seconds until the next fire time (%s)',
                                          wait_time, next_fire_time)
                    else:
                        self.logger.debug('Waiting for any due schedules to appear')
                else:
                    self.logger.debug('Processing more schedules on the next iteration')

                with move_on_after(wait_time):
                    await self._wakeup_event.wait()
                    self._wakeup_event = anyio.Event()
        except get_cancelled_exc_class():
            pass
        except BaseException as exc:
            self.logger.exception('Scheduler crashed')
            exception = exc
        else:
            self.logger.info('Scheduler stopped')
        finally:
            self._state = RunState.stopped
            with move_on_after(3, shield=True):
                await self._events.publish(SchedulerStopped(exception=exception))
