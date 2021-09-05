from __future__ import annotations

import os
import platform
import threading
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from logging import Logger, getLogger
from typing import Any, Callable, Iterable, Mapping, Optional, Type
from uuid import uuid4

from ..abc import DataStore, EventSource, Trigger
from ..datastores.sync.memory import MemoryDataStore
from ..enums import CoalescePolicy, ConflictPolicy, RunState
from ..events import (
    Event, EventHub, ScheduleAdded, SchedulerStarted, SchedulerStopped, ScheduleUpdated,
    SubscriptionToken)
from ..marshalling import callable_to_ref
from ..structures import Job, Schedule, Task
from ..workers.sync import Worker


class Scheduler(EventSource):
    """A synchronous scheduler implementation."""

    _state: RunState = RunState.stopped
    _wakeup_event: threading.Event
    _worker: Optional[Worker] = None

    def __init__(self, data_store: Optional[DataStore] = None, *, identity: Optional[str] = None,
                 logger: Optional[Logger] = None, start_worker: bool = True):
        self.identity = identity or f'{platform.node()}-{os.getpid()}-{id(self)}'
        self.logger = logger or getLogger(__name__)
        self.start_worker = start_worker
        self.data_store = data_store or MemoryDataStore()
        self._exit_stack = ExitStack()
        self._executor = ThreadPoolExecutor(max_workers=1)
        self._events = EventHub()

    @property
    def state(self) -> RunState:
        return self._state

    @property
    def worker(self) -> Optional[Worker]:
        return self._worker

    def __enter__(self) -> Scheduler:
        self._state = RunState.starting
        self._wakeup_event = threading.Event()
        self._exit_stack.__enter__()
        self._exit_stack.enter_context(self._events)

        # Initialize the data store
        self._exit_stack.enter_context(self.data_store)
        relay_token = self._events.relay_events_from(self.data_store)
        self._exit_stack.callback(self.data_store.unsubscribe, relay_token)

        # Wake up the scheduler if the data store emits a significant schedule event
        wakeup_token = self.data_store.subscribe(
            lambda event: self._wakeup_event.set(), {ScheduleAdded, ScheduleUpdated})
        self._exit_stack.callback(self.data_store.unsubscribe, wakeup_token)

        # Start the built-in worker, if configured to do so
        if self.start_worker:
            self._worker = Worker(self.data_store)
            self._exit_stack.enter_context(self._worker)

        # Start the worker and return when it has signalled readiness or raised an exception
        start_future: Future[Event] = Future()
        token = self._events.subscribe(start_future.set_result)
        run_future = self._executor.submit(self.run)
        try:
            wait([start_future, run_future], return_when=FIRST_COMPLETED)
        finally:
            self._events.unsubscribe(token)

        if run_future.done():
            run_future.result()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._state = RunState.stopping
        self._wakeup_event.set()
        self._executor.shutdown(wait=exc_type is None)
        self._exit_stack.__exit__(exc_type, exc_val, exc_tb)
        del self._wakeup_event

    def subscribe(self, callback: Callable[[Event], Any],
                  event_types: Optional[Iterable[Type[Event]]] = None) -> SubscriptionToken:
        return self._events.subscribe(callback, event_types)

    def unsubscribe(self, token: SubscriptionToken) -> None:
        self._events.unsubscribe(token)

    def add_schedule(
        self, func_or_task_id: str | Callable, trigger: Trigger, *, id: Optional[str] = None,
        args: Optional[Iterable] = None, kwargs: Optional[Mapping[str, Any]] = None,
        coalesce: CoalescePolicy = CoalescePolicy.latest,
        misfire_grace_time: float | timedelta | None = None,
        tags: Optional[Iterable[str]] = None,
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
            self.data_store.add_task(task)
        else:
            task = self.data_store.get_task(func_or_task_id)

        schedule = Schedule(id=id, task_id=task.id, trigger=trigger, args=args, kwargs=kwargs,
                            coalesce=coalesce, misfire_grace_time=misfire_grace_time, tags=tags)
        schedule.next_fire_time = trigger.next()
        self.data_store.add_schedule(schedule, conflict_policy)
        self.logger.info('Added new schedule (task=%r, trigger=%r); next run time at %s', task,
                         trigger, schedule.next_fire_time)
        return schedule.id

    def remove_schedule(self, schedule_id: str) -> None:
        self.data_store.remove_schedules({schedule_id})

    def run(self) -> None:
        if self._state is not RunState.starting:
            raise RuntimeError(f'This function cannot be called while the scheduler is in the '
                               f'{self._state} state')

        # Signal that the scheduler has started
        self._state = RunState.started
        self._events.publish(SchedulerStarted())

        try:
            while self._state is RunState.started:
                schedules = self.data_store.acquire_schedules(self.identity, 100)
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
                    for fire_time in fire_times:
                        schedule.last_fire_time = fire_time
                        job = Job(task_id=schedule.task_id, args=schedule.args,
                                  kwargs=schedule.kwargs, schedule_id=schedule.id,
                                  scheduled_fire_time=fire_time,
                                  start_deadline=schedule.next_deadline, tags=schedule.tags)
                        self.data_store.add_job(job)

                # Update the schedules (and release the scheduler's claim on them)
                self.data_store.release_schedules(self.identity, schedules)

                # If we received fewer schedules than the maximum amount, sleep until the next
                # schedule is due or the scheduler is explicitly woken up
                wait_time = None
                if len(schedules) < 100:
                    next_fire_time = self.data_store.get_next_schedule_run_time()
                    if next_fire_time:
                        wait_time = (datetime.now(timezone.utc) - next_fire_time).total_seconds()

                if self._wakeup_event.wait(wait_time):
                    self._wakeup_event = threading.Event()
        except BaseException as exc:
            self._state = RunState.stopped
            self._events.publish(SchedulerStopped(exception=exc))
            raise

        self._state = RunState.stopped
        self._events.publish(SchedulerStopped())

    # def stop(self) -> None:
    #     self.portal.call(self._scheduler.stop)
    #
    # def wait_until_stopped(self) -> None:
    #     self.portal.call(self._scheduler.wait_until_stopped)
