from __future__ import annotations

import os
import platform
import random
import threading
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from logging import Logger, getLogger
from typing import Any, Callable, Iterable, Mapping, cast
from uuid import UUID, uuid4

import attrs

from ..abc import DataStore, EventSource, Trigger
from ..context import current_scheduler
from ..datastores.memory import MemoryDataStore
from ..enums import CoalescePolicy, ConflictPolicy, JobOutcome, RunState
from ..eventbrokers.local import LocalEventBroker
from ..events import (
    Event,
    JobReleased,
    ScheduleAdded,
    SchedulerStarted,
    SchedulerStopped,
    ScheduleUpdated,
)
from ..exceptions import JobCancelled, JobDeadlineMissed, JobLookupError
from ..marshalling import callable_to_ref
from ..structures import Job, JobResult, Schedule, Task
from ..workers.sync import Worker

_microsecond_delta = timedelta(microseconds=1)
_zero_timedelta = timedelta()


@attrs.define(eq=False)
class Scheduler:
    """A synchronous scheduler implementation."""

    data_store: DataStore = attrs.field(factory=MemoryDataStore)
    identity: str = attrs.field(kw_only=True, default=None)
    start_worker: bool = attrs.field(kw_only=True, default=True)
    logger: Logger | None = attrs.field(kw_only=True, default=getLogger(__name__))

    _state: RunState = attrs.field(init=False, default=RunState.stopped)
    _wakeup_event: threading.Event = attrs.field(init=False)
    _wakeup_deadline: datetime | None = attrs.field(init=False, default=None)
    _worker: Worker | None = attrs.field(init=False, default=None)
    _events: LocalEventBroker = attrs.field(init=False, factory=LocalEventBroker)
    _exit_stack: ExitStack = attrs.field(init=False)

    def __attrs_post_init__(self) -> None:
        if not self.identity:
            self.identity = f"{platform.node()}-{os.getpid()}-{id(self)}"

    @property
    def events(self) -> EventSource:
        return self._events

    @property
    def state(self) -> RunState:
        return self._state

    @property
    def worker(self) -> Worker | None:
        return self._worker

    def __enter__(self) -> Scheduler:
        self._state = RunState.starting
        self._wakeup_event = threading.Event()
        self._exit_stack = ExitStack()
        self._exit_stack.__enter__()
        self._exit_stack.enter_context(self._events)

        # Initialize the data store and start relaying events to the scheduler's event broker
        self._exit_stack.enter_context(self.data_store)
        self._exit_stack.enter_context(
            self.data_store.events.subscribe(self._events.publish)
        )

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
                self._worker = Worker(self.data_store)
                self._exit_stack.enter_context(self._worker)
            finally:
                current_scheduler.reset(token)

        # Start the scheduler and return when it has signalled readiness or raised an exception
        start_future: Future[Event] = Future()
        with self._events.subscribe(start_future.set_result, one_shot=True):
            executor = ThreadPoolExecutor(1)
            self._exit_stack.push(
                lambda exc_type, *args: executor.shutdown(wait=exc_type is None)
            )
            run_future = executor.submit(self.run)
            wait([start_future, run_future], return_when=FIRST_COMPLETED)

        if run_future.done():
            run_future.result()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._state = RunState.stopping
        self._wakeup_event.set()
        self._exit_stack.__exit__(exc_type, exc_val, exc_tb)
        self._state = RunState.stopped
        del self._wakeup_event

    def _schedule_added_or_modified(self, event: Event) -> None:
        event_ = cast("ScheduleAdded | ScheduleUpdated", event)
        if not self._wakeup_deadline or (
            event_.next_fire_time and event_.next_fire_time < self._wakeup_deadline
        ):
            self.logger.debug(
                "Detected a %s event – waking up the scheduler", type(event).__name__
            )
            self._wakeup_event.set()

    def add_schedule(
        self,
        func_or_task_id: str | Callable,
        trigger: Trigger,
        *,
        id: str | None = None,
        args: Iterable | None = None,
        kwargs: Mapping[str, Any] | None = None,
        coalesce: CoalescePolicy = CoalescePolicy.latest,
        misfire_grace_time: float | timedelta | None = None,
        max_jitter: float | timedelta | None = None,
        tags: Iterable[str] | None = None,
        conflict_policy: ConflictPolicy = ConflictPolicy.do_nothing,
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

        schedule = Schedule(
            id=id,
            task_id=task.id,
            trigger=trigger,
            args=args,
            kwargs=kwargs,
            coalesce=coalesce,
            misfire_grace_time=misfire_grace_time,
            max_jitter=max_jitter,
            tags=tags,
        )
        schedule.next_fire_time = trigger.next()
        self.data_store.add_schedule(schedule, conflict_policy)
        self.logger.info(
            "Added new schedule (task=%r, trigger=%r); next run time at %s",
            task,
            trigger,
            schedule.next_fire_time,
        )
        return schedule.id

    def get_schedule(self, id: str) -> Schedule:
        schedules = self.data_store.get_schedules({id})
        return schedules[0]

    def remove_schedule(self, schedule_id: str) -> None:
        self.data_store.remove_schedules({schedule_id})

    def add_job(
        self,
        func_or_task_id: str | Callable,
        *,
        args: Iterable | None = None,
        kwargs: Mapping[str, Any] | None = None,
        tags: Iterable[str] | None = None,
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
            self.data_store.add_task(task)
        else:
            task = self.data_store.get_task(func_or_task_id)

        job = Job(
            task_id=task.id,
            args=args or (),
            kwargs=kwargs or {},
            tags=tags or frozenset(),
        )
        self.data_store.add_job(job)
        return job.id

    def get_job_result(self, job_id: UUID, *, wait: bool = True) -> JobResult:
        """
        Retrieve the result of a job.

        :param job_id: the ID of the job
        :param wait: if ``True``, wait until the job has ended (one way or another), ``False`` to
                     raise an exception if the result is not yet available
        :raises JobLookupError: if the job does not exist in the data store

        """
        wait_event = threading.Event()

        def listener(event: JobReleased) -> None:
            if event.job_id == job_id:
                wait_event.set()

        with self.data_store.events.subscribe(listener, {JobReleased}):
            result = self.data_store.get_job_result(job_id)
            if result:
                return result
            elif not wait:
                raise JobLookupError(job_id)

            wait_event.wait()

        result = self.data_store.get_job_result(job_id)
        assert isinstance(result, JobResult)
        return result

    def run_job(
        self,
        func_or_task_id: str | Callable,
        *,
        args: Iterable | None = None,
        kwargs: Mapping[str, Any] | None = None,
        tags: Iterable[str] | None = (),
    ) -> Any:
        """
        Convenience method to add a job and then return its result (or raise its exception).

        :returns: the return value of the target function

        """
        job_complete_event = threading.Event()

        def listener(event: JobReleased) -> None:
            if event.job_id == job_id:
                job_complete_event.set()

        job_id: UUID | None = None
        with self.data_store.events.subscribe(listener, {JobReleased}):
            job_id = self.add_job(func_or_task_id, args=args, kwargs=kwargs, tags=tags)
            job_complete_event.wait()

        result = self.get_job_result(job_id)
        if result.outcome is JobOutcome.success:
            return result.return_value
        elif result.outcome is JobOutcome.error:
            raise result.exception
        elif result.outcome is JobOutcome.missed_start_deadline:
            raise JobDeadlineMissed
        elif result.outcome is JobOutcome.cancelled:
            raise JobCancelled
        else:
            raise RuntimeError(f"Unknown job outcome: {result.outcome}")

    def run(self) -> None:
        if self._state is not RunState.starting:
            raise RuntimeError(
                f"This function cannot be called while the scheduler is in the "
                f"{self._state} state"
            )

        # Signal that the scheduler has started
        self._state = RunState.started
        self._events.publish(SchedulerStarted())

        try:
            while self._state is RunState.started:
                schedules = self.data_store.acquire_schedules(self.identity, 100)
                self.logger.debug(
                    "Processing %d schedules retrieved from the data store",
                    len(schedules),
                )
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
                                "Error computing next fire time for schedule %r of task %r – "
                                "removing schedule",
                                schedule.id,
                                schedule.task_id,
                            )
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
                    max_jitter = (
                        schedule.max_jitter.total_seconds()
                        if schedule.max_jitter
                        else 0
                    )
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
                                jitter_s = min(
                                    [
                                        max_jitter,
                                        (
                                            next_fire_time
                                            - fire_time
                                            - _microsecond_delta
                                        ).total_seconds(),
                                    ]
                                )
                                jitter = timedelta(seconds=random.uniform(0, jitter_s))
                                fire_time += jitter

                        schedule.last_fire_time = fire_time
                        job = Job(
                            task_id=schedule.task_id,
                            args=schedule.args,
                            kwargs=schedule.kwargs,
                            schedule_id=schedule.id,
                            scheduled_fire_time=fire_time,
                            jitter=jitter,
                            start_deadline=schedule.next_deadline,
                            tags=schedule.tags,
                        )
                        self.data_store.add_job(job)

                # Update the schedules (and release the scheduler's claim on them)
                self.data_store.release_schedules(self.identity, schedules)

                # If we received fewer schedules than the maximum amount, sleep until the next
                # schedule is due or the scheduler is explicitly woken up
                wait_time = None
                if len(schedules) < 100:
                    self._wakeup_deadline = self.data_store.get_next_schedule_run_time()
                    if self._wakeup_deadline:
                        wait_time = (
                            self._wakeup_deadline - datetime.now(timezone.utc)
                        ).total_seconds()
                        self.logger.debug(
                            "Sleeping %.3f seconds until the next fire time (%s)",
                            wait_time,
                            self._wakeup_deadline,
                        )
                    else:
                        self.logger.debug("Waiting for any due schedules to appear")

                    if self._wakeup_event.wait(wait_time):
                        self._wakeup_event = threading.Event()
                else:
                    self.logger.debug("Processing more schedules on the next iteration")
        except BaseException as exc:
            self._state = RunState.stopped
            self.logger.exception("Scheduler crashed")
            self._events.publish(SchedulerStopped(exception=exc))
            raise

        self._state = RunState.stopped
        self.logger.info("Scheduler stopped")
        self._events.publish(SchedulerStopped())

    # def stop(self) -> None:
    #     self.portal.call(self._scheduler.stop)
    #
    # def wait_until_stopped(self) -> None:
    #     self.portal.call(self._scheduler.wait_until_stopped)
