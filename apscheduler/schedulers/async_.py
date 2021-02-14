import os
import platform
import threading
from contextlib import AsyncExitStack
from datetime import datetime, timedelta, timezone
from logging import Logger, getLogger
from typing import Any, Callable, Dict, Iterable, Mapping, Optional, Union
from uuid import uuid4

from anyio import create_event, create_task_group, get_cancelled_exc_class, open_cancel_scope
from anyio.abc import CancelScope, Event, TaskGroup

from ..abc import DataStore, Job, Schedule, Task, Trigger
from ..datastores.memory import MemoryDataStore
from ..events import EventHub
from ..marshalling import callable_to_ref
from ..policies import CoalescePolicy, ConflictPolicy
from ..workers.async_ import AsyncWorker


class AsyncScheduler(EventHub):
    _task_group: Optional[TaskGroup] = None
    _stop_event: Optional[Event] = None
    _running: bool = False
    _worker: Optional[AsyncWorker] = None
    _acquire_cancel_scope: Optional[CancelScope] = None

    def __init__(self, data_store: Optional[DataStore] = None, *, identity: Optional[str] = None,
                 logger: Optional[Logger] = None, start_worker: bool = True):
        super().__init__()
        self.data_store = data_store or MemoryDataStore()
        self.identity = identity or f'{platform.node()}-{os.getpid()}-{threading.get_ident()}'
        self.logger = logger or getLogger(__name__)
        self.start_worker = start_worker
        self._tasks: Dict[str, Task] = {}
        self._exit_stack = AsyncExitStack()

    @property
    def worker(self) -> Optional[AsyncWorker]:
        return self._worker

    async def __aenter__(self):
        await self._exit_stack.__aenter__()

        # Start the built-in worker, if configured to do so
        if self.start_worker:
            # The worker handles initializing the data store
            self._worker = AsyncWorker(self.data_store)
            await self._exit_stack.enter_async_context(self._worker)
        else:
            # Otherwise, initialize the data store ourselves
            await self._exit_stack.enter_async_context(self.data_store)

        # Start the actual scheduler
        self._task_group = create_task_group()
        await self._exit_stack.enter_async_context(self._task_group)
        start_event = create_event()
        await self._task_group.spawn(self.run, start_event)
        await start_event.wait()

        # await self.start(self._task_group)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Exit gracefully (wait for ongoing tasks to finish) if the context exited without errors
        await self.stop(force=exc_type is not None)
        if self._worker:
            await self._worker.stop(force=exc_type is not None)

        await self._exit_stack.__aexit__(exc_type, exc_val, exc_tb)

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
                            coalesce=coalesce, misfire_grace_time=misfire_grace_time, tags=tags,
                            next_fire_time=trigger.next())
        await self.data_store.add_schedule(schedule, conflict_policy)
        self.logger.info('Added new schedule (task=%r, trigger=%r); next run time at %s', taskdef,
                         trigger, schedule.next_fire_time)
        return schedule.id

    async def remove_schedule(self, schedule_id: str) -> None:
        await self.data_store.remove_schedules({schedule_id})

    async def run(self, start_event: Optional[Event] = None) -> None:
        self._stop_event = create_event()
        self._running = True
        if start_event:
            await start_event.set()

        while self._running:
            async with open_cancel_scope() as self._acquire_cancel_scope:
                try:
                    schedules = await self.data_store.acquire_schedules(self.identity, 100)
                except get_cancelled_exc_class():
                    break
                finally:
                    del self._acquire_cancel_scope

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
                    job = Job(taskdef.id, taskdef.func, schedule.args, schedule.kwargs,
                              schedule.id, fire_time, schedule.next_deadline,
                              schedule.tags)
                    print('Added job', job.id, 'for schedule', schedule)
                    await self.data_store.add_job(job)

            self.logger.debug('Releasing %d schedules', len(schedules))
            await self.data_store.release_schedules(self.identity, schedules)

        await self._stop_event.set()
        del self._stop_event

    # async def start(self, task_group: TaskGroup, *, reset_datastore: bool = False) -> None:
    #     start_event = create_event()
    #     await task_group.spawn(self.run, start_event)
    #     await start_event.wait()
    #
    #     if self.start_worker:
    #         self._worker = AsyncWorker(self.data_store)
    #         await self._worker.start(task_group)

    async def stop(self, force: bool = False) -> None:
        self._running = False
        if self._worker:
            await self._worker.stop(force)

        if self._acquire_cancel_scope:
            await self._acquire_cancel_scope.cancel()
        if force and self._task_group:
            await self._task_group.cancel_scope.cancel()

    async def wait_until_stopped(self) -> None:
        if self._stop_event:
            await self._stop_event.wait()
