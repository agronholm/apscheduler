from __future__ import annotations

import os
import platform
from contextlib import AsyncExitStack
from datetime import datetime, timedelta, timezone
from logging import Logger, getLogger
from typing import Any, Callable, Dict, Iterable, Mapping, Optional, Type, Union
from uuid import uuid4

import anyio
from anyio import (
    TASK_STATUS_IGNORED, Event, create_task_group, get_cancelled_exc_class, move_on_after)
from anyio.abc import TaskGroup

from ..abc import AsyncDataStore, DataStore, EventSource, Job, Schedule, Trigger
from ..datastores.async_.sync_adapter import AsyncDataStoreAdapter
from ..datastores.sync.memory import MemoryDataStore
from ..enums import RunState
from ..events import (
    AsyncEventHub, ScheduleAdded, SchedulerStarted, SchedulerStopped, ScheduleUpdated,
    SubscriptionToken)
from ..marshalling import callable_to_ref
from ..policies import CoalescePolicy, ConflictPolicy
from ..structures import Task
from ..workers.async_ import AsyncWorker


class AsyncScheduler(EventSource):
    """An asynchronous (AnyIO based) scheduler implementation."""

    _state: RunState = RunState.stopped
    _wakeup_event: anyio.Event
    _worker: Optional[AsyncWorker] = None
    _task_group: Optional[TaskGroup] = None

    def __init__(self, data_store: Union[DataStore, AsyncDataStore] = None, *,
                 identity: Optional[str] = None, logger: Optional[Logger] = None,
                 start_worker: bool = True):
        self.identity = identity or f'{platform.node()}-{os.getpid()}-{id(self)}'
        self.logger = logger or getLogger(__name__)
        self.start_worker = start_worker
        self._tasks: Dict[str, Task] = {}
        self._exit_stack = AsyncExitStack()
        self._events = AsyncEventHub()

        data_store = data_store or MemoryDataStore()
        if isinstance(data_store, DataStore):
            self.data_store = AsyncDataStoreAdapter(data_store)
        else:
            self.data_store = data_store

    @property
    def worker(self) -> Optional[AsyncWorker]:
        return self._worker

    async def __aenter__(self):
        self._state = RunState.starting
        self._wakeup_event = anyio.Event()
        await self._exit_stack.__aenter__()
        await self._exit_stack.enter_async_context(self._events)

        # Initialize the data store
        await self._exit_stack.enter_async_context(self.data_store)
        relay_token = self._events.relay_events_from(self.data_store)
        self._exit_stack.callback(self.data_store.unsubscribe, relay_token)

        # Wake up the scheduler if the data store emits a significant schedule event
        wakeup_token = self.data_store.subscribe(
            lambda event: self._wakeup_event.set(), {ScheduleAdded, ScheduleUpdated})
        self._exit_stack.callback(self.data_store.unsubscribe, wakeup_token)

        # Start the built-in worker, if configured to do so
        if self.start_worker:
            self._worker = AsyncWorker(self.data_store)
            await self._exit_stack.enter_async_context(self._worker)

        # Start the worker and return when it has signalled readiness or raised an exception
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

    def _get_taskdef(self, func_or_id: Union[str, Callable]) -> Task:
        task_id = func_or_id if isinstance(func_or_id, str) else callable_to_ref(func_or_id)
        taskdef = self._tasks.get(task_id)
        if not taskdef:
            if isinstance(func_or_id, str):
                raise LookupError('no task found with ID {!r}'.format(func_or_id))
            else:
                taskdef = self._tasks[task_id] = Task(id=task_id, func=func_or_id)

        return taskdef

    def define_task(self, func: Callable, task_id: Optional[str] = None, **kwargs):
        if task_id is None:
            task_id = callable_to_ref(func)

        task = Task(id=task_id, **kwargs)
        if self._tasks.setdefault(task_id, task) is not task:
            pass

    async def add_schedule(
        self, task: Union[str, Callable], trigger: Trigger, *, id: Optional[str] = None,
        args: Optional[Iterable] = None, kwargs: Optional[Mapping[str, Any]] = None,
        coalesce: CoalescePolicy = CoalescePolicy.latest,
        misfire_grace_time: Union[float, timedelta, None] = None,
        tags: Optional[Iterable[str]] = None,
        conflict_policy: ConflictPolicy = ConflictPolicy.do_nothing
    ) -> str:
        id = id or str(uuid4())
        args = tuple(args or ())
        kwargs = dict(kwargs or {})
        tags = frozenset(tags or ())
        if isinstance(misfire_grace_time, (int, float)):
            misfire_grace_time = timedelta(seconds=misfire_grace_time)

        taskdef = self._get_taskdef(task)
        schedule = Schedule(id=id, task_id=taskdef.id, trigger=trigger, args=args, kwargs=kwargs,
                            coalesce=coalesce, misfire_grace_time=misfire_grace_time, tags=tags)
        schedule.next_fire_time = trigger.next()
        await self.data_store.add_schedule(schedule, conflict_policy)
        self.logger.info('Added new schedule (task=%r, trigger=%r); next run time at %s', taskdef,
                         trigger, schedule.next_fire_time)
        return schedule.id

    async def remove_schedule(self, schedule_id: str) -> None:
        await self.data_store.remove_schedules({schedule_id})

    async def run(self, *, task_status=TASK_STATUS_IGNORED) -> None:
        if self._state is not RunState.starting:
            raise RuntimeError(f'This function cannot be called while the scheduler is in the '
                               f'{self._state} state')

        # Signal that the scheduler has started
        self._state = RunState.started
        task_status.started()
        self._events.publish(SchedulerStarted())

        try:
            while self._state is RunState.started:
                schedules = await self.data_store.acquire_schedules(self.identity, 100)
                now = datetime.now(timezone.utc)
                for schedule in schedules:
                    # Look up the task definition
                    try:
                        taskdef = self._get_taskdef(schedule.task_id)
                    except LookupError:
                        self.logger.error('Cannot locate task definition %r for schedule %r – '
                                          'removing schedule', schedule.task_id, schedule.id)
                        schedule.next_fire_time = None
                        continue

                    # Calculate a next fire time for the schedule, if possible
                    fire_times = [schedule.next_fire_time]
                    calculate_next = schedule.trigger.next
                    while True:
                        try:
                            fire_time = calculate_next()
                        except Exception:
                            self.logger.exception(
                                'Error computing next fire time for schedule %r of task %r – '
                                'removing schedule', schedule.id, taskdef.id)
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
                    for fire_time in fire_times:
                        schedule.last_fire_time = fire_time
                        job = Job(task_id=taskdef.id, func=taskdef.func, args=schedule.args,
                                  kwargs=schedule.kwargs, schedule_id=schedule.id,
                                  scheduled_fire_time=fire_time,
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
                        wait_time = (datetime.now(timezone.utc) - next_fire_time).total_seconds()

                with move_on_after(wait_time):
                    await self._wakeup_event.wait()

                self._wakeup_event = anyio.Event()
        except get_cancelled_exc_class():
            pass
        except BaseException as exc:
            self._state = RunState.stopped
            self._events.publish(SchedulerStopped(exception=exc))
            raise

        self._state = RunState.stopped
        self._events.publish(SchedulerStopped())

    # async def stop(self, force: bool = False) -> None:
    #     self._running = False
    #     if self._worker:
    #         await self._worker.stop(force)
    #
    #     if self._acquire_cancel_scope:
    #         self._acquire_cancel_scope.cancel()
    #     if force and self._task_group:
    #         self._task_group.cancel_scope.cancel()
    #
    # async def wait_until_stopped(self) -> None:
    #     if self._stop_event:
    #         await self._stop_event.wait()
