import logging
import os
import platform
import threading
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone, tzinfo
from traceback import format_exc
from typing import Any, Callable, Dict, Iterable, Mapping, Optional, Set, Union
from uuid import uuid4

import tzlocal
from anyio import create_event, create_task_group, move_on_after
from anyio.abc import Event

from ..abc import DataStore, EventHub, EventSource, Executor, Job, Schedule, Task, Trigger
from ..datastores.memory import MemoryScheduleStore
from ..eventhubs.local import LocalEventHub
from ..events import JobSubmissionFailed, SchedulesAdded, SchedulesUpdated
from ..marshalling import callable_to_ref
from ..validators import as_timezone
from ..workers.local import LocalExecutor


@dataclass
class AsyncScheduler(EventSource):
    schedule_store: DataStore = field(default_factory=MemoryScheduleStore)
    worker: Executor = field(default_factory=LocalExecutor)
    timezone: tzinfo = field(default_factory=tzlocal.get_localzone)
    identity: str = f'{platform.node()}-{os.getpid()}-{threading.get_ident()}'
    logger: logging.Logger = field(default_factory=lambda: logging.getLogger(__name__))
    _event_hub: EventHub = field(init=False, default_factory=LocalEventHub)
    _tasks: Dict[str, Task] = field(init=False, default_factory=dict)
    _async_stack: AsyncExitStack = field(init=False, default_factory=AsyncExitStack)
    _next_fire_time: Optional[datetime] = field(init=False, default=None)
    _wakeup_event: Optional[Event] = field(init=False, default=None)
    _closed: bool = field(init=False, default=False)

    def __post_init__(self):
        self.timezone = as_timezone(self.timezone)

    async def __aenter__(self):
        await self._async_stack.__aenter__()
        task_group = create_task_group()
        await self._async_stack.enter_async_context(task_group)
        await task_group.spawn(self.run)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._async_stack.__aexit__(exc_type, exc_val, exc_tb)

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
        coalesce: bool = True, misfire_grace_time: Union[float, timedelta, None] = None,
        tags: Optional[Iterable[str]] = None
    ) -> str:
        id = id or str(uuid4())
        args = tuple(args or ())
        kwargs = dict(kwargs or {})
        tags = frozenset(tags or ())
        if isinstance(misfire_grace_time, (int, float)):
            misfire_grace_time = timedelta(seconds=misfire_grace_time)

        taskdef = self._get_taskdef(task)
        schedule = Schedule(id=id, task_id=taskdef.id, trigger=trigger, args=args, kwargs=kwargs,
                            coalesce=coalesce, misfire_grace_time=misfire_grace_time, tags=tags,
                            next_fire_time=trigger.next())
        await self.schedule_store.add_or_replace_schedules([schedule])
        self.logger.info('Added new schedule for task %s; next run time at %s', taskdef,
                         schedule.next_fire_time)
        return schedule.id

    async def remove_schedule(self, schedule_id: str) -> None:
        await self.schedule_store.remove_schedules({schedule_id})

    async def _handle_worker_event(self, event: Event) -> None:
        await self._event_hub.publish(event)

    async def _handle_datastore_event(self, event: Event) -> None:
        if isinstance(event, (SchedulesAdded, SchedulesUpdated)):
            # Wake up the scheduler if any schedule has an earlier next fire time than the one
            # we're currently waiting for
            if event.earliest_next_fire_time:
                if (self._next_fire_time is None
                        or self._next_fire_time > event.earliest_next_fire_time):
                    self.logger.debug('Job store reported an updated next fire time that requires '
                                      'the scheduler to wake up: %s',
                                      event.earliest_next_fire_time)
                    await self.wakeup()

        await self._event_hub.publish(event)

    async def _process_schedules(self):
        async with self.schedule_store.acquire_due_schedules(
                self.identity, datetime.now(timezone.utc)) as schedules:
            schedule_ids_to_remove: Set[str] = set()
            schedule_updates: Dict[str, Dict[str, Any]] = {}
            for schedule in schedules:
                # Look up the task definition
                try:
                    taskdef = self._get_taskdef(schedule.task_id)
                except LookupError:
                    self.logger.error('Cannot locate task definition %r for schedule %r – '
                                      'removing schedule', schedule.task_id, schedule.id)
                    schedule_ids_to_remove.add(schedule.id)
                    continue

                # Calculate a next fire time for the schedule, if possible
                try:
                    next_fire_time = schedule.trigger.next()
                except Exception:
                    self.logger.exception('Error computing next fire time for schedule %r of task '
                                          '%r – removing schedule', schedule.id, taskdef.id)
                    next_fire_time = None

                # Queue a schedule update if a next fire time could be calculated.
                # Otherwise, queue the schedule for removal.
                if next_fire_time:
                    schedule_updates[schedule.id] = {
                        'next_fire_time': next_fire_time,
                        'last_fire_time': schedule.next_fire_time,
                        'trigger': schedule.trigger
                    }
                else:
                    schedule_ids_to_remove.add(schedule.id)

                # Submit a new job to the executor
                job = Job(taskdef.id, taskdef.func, schedule.args, schedule.kwargs, schedule.id,
                          schedule.last_fire_time, schedule.next_deadline, schedule.tags)
                try:
                    await self.worker.submit_job(job)
                except Exception as exc:
                    self.logger.exception('Error submitting job to worker')
                    event = JobSubmissionFailed(datetime.now(self.timezone), job.id, job.task_id,
                                                job.schedule_id, job.scheduled_start_time,
                                                formatted_traceback=format_exc(), exception=exc)
                    await self._event_hub.publish(event)

            # Removed finished schedules
            if schedule_ids_to_remove:
                await self.schedule_store.remove_schedules(schedule_ids_to_remove)

            # Update the next fire times
            if schedule_updates:
                await self.schedule_store.update_schedules(schedule_updates)

        return await self.schedule_store.get_next_fire_time()

    async def run(self):
        async with AsyncExitStack() as stack:
            await stack.enter_async_context(self.schedule_store)
            await stack.enter_async_context(self.worker)
            await self.worker.subscribe(self._handle_worker_event)
            await self.schedule_store.subscribe(self._handle_datastore_event)
            self._wakeup_event = create_event()
            while not self._closed:
                self._next_fire_time = await self._process_schedules()

                if self._next_fire_time:
                    wait_time = (self._next_fire_time - datetime.now(timezone.utc)).total_seconds()
                else:
                    wait_time = float('inf')

                self._wakeup_event = create_event()
                async with move_on_after(wait_time):
                    await self._wakeup_event.wait()

    async def wakeup(self) -> None:
        await self._wakeup_event.set()

    async def shutdown(self, force: bool = False) -> None:
        if not self._closed:
            self._closed = True
            await self.wakeup()

    async def subscribe(self, callback: Callable[[Event], Any]) -> None:
        await self._event_hub.subscribe(callback)
