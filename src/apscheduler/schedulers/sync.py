from __future__ import annotations

import atexit
import os
import platform
import random
import sys
import threading
from concurrent.futures import Future
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from logging import Logger, getLogger
from types import TracebackType
from typing import Any, Callable, Iterable, Mapping, cast
from uuid import UUID, uuid4

import attrs

from .._context import current_scheduler
from .._enums import CoalescePolicy, ConflictPolicy, JobOutcome, RunState
from .._events import (
    Event,
    JobReleased,
    ScheduleAdded,
    SchedulerStarted,
    SchedulerStopped,
    ScheduleUpdated,
)
from .._exceptions import (
    JobCancelled,
    JobDeadlineMissed,
    JobLookupError,
    ScheduleLookupError,
)
from .._structures import Job, JobResult, Schedule, Task
from ..abc import DataStore, EventBroker, Trigger
from ..datastores.memory import MemoryDataStore
from ..eventbrokers.local import LocalEventBroker
from ..marshalling import callable_to_ref
from ..workers.sync import Worker

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

_microsecond_delta = timedelta(microseconds=1)
_zero_timedelta = timedelta()


@attrs.define(eq=False)
class Scheduler:
    """A synchronous scheduler implementation."""

    data_store: DataStore = attrs.field(factory=MemoryDataStore)
    event_broker: EventBroker = attrs.field(factory=LocalEventBroker)
    identity: str = attrs.field(kw_only=True, default=None)
    start_worker: bool = attrs.field(kw_only=True, default=True)
    logger: Logger | None = attrs.field(kw_only=True, default=getLogger(__name__))

    _state: RunState = attrs.field(init=False, default=RunState.stopped)
    _thread: threading.Thread | None = attrs.field(init=False, default=None)
    _wakeup_event: threading.Event = attrs.field(init=False, factory=threading.Event)
    _wakeup_deadline: datetime | None = attrs.field(init=False, default=None)
    _services_initialized: bool = attrs.field(init=False, default=False)
    _exit_stack: ExitStack | None = attrs.field(init=False, default=None)
    _lock: threading.RLock = attrs.field(init=False, factory=threading.RLock)

    def __attrs_post_init__(self) -> None:
        if not self.identity:
            self.identity = f"{platform.node()}-{os.getpid()}-{id(self)}"

    def __enter__(self: Self) -> Self:
        self._exit_stack = ExitStack()
        self._ensure_services_ready(self._exit_stack)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ) -> None:
        self.stop()
        self._exit_stack.__exit__(exc_type, exc_val, exc_tb)

    def _ensure_services_ready(self, exit_stack: ExitStack | None = None) -> None:
        """Ensure that the data store and event broker have been initialized."""
        with self._lock:
            if not self._services_initialized:
                if exit_stack is None:
                    if self._exit_stack is None:
                        self._exit_stack = exit_stack = ExitStack()
                        atexit.register(self._exit_stack.close)
                    else:
                        exit_stack = self._exit_stack

                self._services_initialized = True
                exit_stack.callback(setattr, self, "_services_initialized", False)

                self.event_broker.start()
                exit_stack.push(
                    lambda *exc_info: self.event_broker.stop(
                        force=exc_info[0] is not None
                    )
                )

                # Initialize the data store
                self.data_store.start(self.event_broker)
                exit_stack.push(
                    lambda *exc_info: self.data_store.stop(
                        force=exc_info[0] is not None
                    )
                )

    def _schedule_added_or_modified(self, event: Event) -> None:
        event_ = cast("ScheduleAdded | ScheduleUpdated", event)
        if not self._wakeup_deadline or (
            event_.next_fire_time and event_.next_fire_time < self._wakeup_deadline
        ):
            self.logger.debug(
                "Detected a %s event – waking up the scheduler", type(event).__name__
            )
            self._wakeup_event.set()

    def _join_thread(self) -> None:
        if self._thread:
            self._thread.join()
            self._thread = None

    @property
    def state(self) -> RunState:
        """The current running state of the scheduler."""
        return self._state

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
        """
        Schedule a task to be run one or more times in the future.

        :param func_or_task_id: either a callable or an ID of an existing task
            definition
        :param trigger: determines the times when the task should be run
        :param id: an explicit identifier for the schedule (if omitted, a random, UUID
            based ID will be assigned)
        :param args: positional arguments to be passed to the task function
        :param kwargs: keyword arguments to be passed to the task function
        :param coalesce: determines what to do when processing the schedule if multiple
            fire times have become due for this schedule since the last processing
        :param misfire_grace_time: maximum number of seconds the scheduled job's actual
            run time is allowed to be late, compared to the scheduled run time
        :param max_jitter: maximum number of seconds to randomly add to the scheduled
            time for each job created from this schedule
        :param tags: strings that can be used to categorize and filter the schedule and
            its derivative jobs
        :param conflict_policy: determines what to do if a schedule with the same ID
            already exists in the data store
        :return: the ID of the newly added schedule

        """
        self._ensure_services_ready()
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
        """
        Retrieve a schedule from the data store.

        :param id: the unique identifier of the schedule
        :raises ScheduleLookupError: if the schedule could not be found

        """
        self._ensure_services_ready()
        schedules = self.data_store.get_schedules({id})
        if schedules:
            return schedules[0]
        else:
            raise ScheduleLookupError(id)

    def get_schedules(self) -> list[Schedule]:
        """
        Retrieve all schedules from the data store.

        :return: a list of schedules, in an unspecified order

        """
        self._ensure_services_ready()
        return self.data_store.get_schedules()

    def remove_schedule(self, id: str) -> None:
        """
        Remove the given schedule from the data store.

        :param id: the unique identifier of the schedule

        """
        self._ensure_services_ready()
        self.data_store.remove_schedules({id})

    def add_job(
        self,
        func_or_task_id: str | Callable,
        *,
        args: Iterable | None = None,
        kwargs: Mapping[str, Any] | None = None,
        tags: Iterable[str] | None = None,
        result_expiration_time: timedelta | float = 0,
    ) -> UUID:
        """
        Add a job to the data store.

        :param func_or_task_id: either a callable or an ID of an existing task
            definition
        :param args: positional arguments to be passed to the task function
        :param kwargs: keyword arguments to be passed to the task function
        :param tags: strings that can be used to categorize and filter the job
        :param result_expiration_time: the minimum time (as seconds, or timedelta) to
            keep the result of the job available for fetching (the result won't be
            saved at all if that time is 0)
        :return: the ID of the newly created job

        """
        self._ensure_services_ready()
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
            result_expiration_time=result_expiration_time,
        )
        self.data_store.add_job(job)
        return job.id

    def get_job_result(self, job_id: UUID, *, wait: bool = True) -> JobResult:
        """
        Retrieve the result of a job.

        :param job_id: the ID of the job
        :param wait: if ``True``, wait until the job has ended (one way or another),
            ``False`` to raise an exception if the result is not yet available
        :raises JobLookupError: if ``wait=False`` and the job result does not exist in
            the data store

        """
        self._ensure_services_ready()
        wait_event = threading.Event()

        def listener(event: JobReleased) -> None:
            if event.job_id == job_id:
                wait_event.set()

        with self.data_store.events.subscribe(listener, {JobReleased}, one_shot=True):
            result = self.data_store.get_job_result(job_id)
            if result:
                return result
            elif not wait:
                raise JobLookupError(job_id)

            wait_event.wait()

        return self.data_store.get_job_result(job_id)

    def run_job(
        self,
        func_or_task_id: str | Callable,
        *,
        args: Iterable | None = None,
        kwargs: Mapping[str, Any] | None = None,
        tags: Iterable[str] | None = (),
    ) -> Any:
        """
        Convenience method to add a job and then return its result.

        If the job raised an exception, that exception will be reraised here.

        :param func_or_task_id: either a callable or an ID of an existing task
            definition
        :param args: positional arguments to be passed to the task function
        :param kwargs: keyword arguments to be passed to the task function
        :param tags: strings that can be used to categorize and filter the job
        :returns: the return value of the task function

        """
        self._ensure_services_ready()
        job_complete_event = threading.Event()

        def listener(event: JobReleased) -> None:
            if event.job_id == job_id:
                job_complete_event.set()

        job_id: UUID | None = None
        with self.data_store.events.subscribe(listener, {JobReleased}):
            job_id = self.add_job(
                func_or_task_id,
                args=args,
                kwargs=kwargs,
                tags=tags,
                result_expiration_time=timedelta(minutes=15),
            )
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

    def start_in_background(self) -> None:
        """
        Launch the scheduler in a new thread.

        This method registers :mod:`atexit` hooks to shut down the scheduler and wait
        for the thread to finish.

        :raises RuntimeError: if the scheduler is not in the ``stopped`` state

        """
        with self._lock:
            if self._state is not RunState.stopped:
                raise RuntimeError(
                    f'Cannot start the scheduler when it is in the "{self._state}" '
                    f"state"
                )

            self._state = RunState.starting

        start_future: Future[None] = Future()
        self._thread = threading.Thread(
            target=self._run, args=[start_future], daemon=True
        )
        self._thread.start()
        try:
            start_future.result()
        except BaseException:
            self._thread = None
            raise

        self._exit_stack.callback(self._join_thread)
        self._exit_stack.callback(self.stop)

    def stop(self) -> None:
        """
        Signal the scheduler that it should stop processing schedules.

        This method does not wait for the scheduler to actually stop.
        For that, see :meth:`wait_until_stopped`.

        """
        with self._lock:
            if self._state is RunState.started:
                self._state = RunState.stopping
                self._wakeup_event.set()

    def wait_until_stopped(self) -> None:
        """
        Wait until the scheduler is in the "stopped" or "stopping" state.

        If the scheduler is already stopped or in the process of stopping, this method
        returns immediately. Otherwise, it waits until the scheduler posts the
        ``SchedulerStopped`` event.

        """
        with self._lock:
            if self._state in (RunState.stopped, RunState.stopping):
                return

            event = threading.Event()
            sub = self.event_broker.subscribe(
                lambda ev: event.set(), {SchedulerStopped}, one_shot=True
            )

        with sub:
            event.wait()

    def run_until_stopped(self) -> None:
        """
        Run the scheduler (and its internal worker) until it is explicitly stopped.

        This method will only return if :meth:`stop` is called.

        """
        with self._lock:
            if self._state is not RunState.stopped:
                raise RuntimeError(
                    f'Cannot start the scheduler when it is in the "{self._state}" '
                    f"state"
                )

            self._state = RunState.starting

        self._run(None)

    def _run(self, start_future: Future[None] | None) -> None:
        assert self._state is RunState.starting
        with self._exit_stack.pop_all() as exit_stack:
            try:
                self._ensure_services_ready(exit_stack)

                # Wake up the scheduler if the data store emits a significant schedule
                # event
                exit_stack.enter_context(
                    self.data_store.events.subscribe(
                        self._schedule_added_or_modified,
                        {ScheduleAdded, ScheduleUpdated},
                    )
                )

                # Start the built-in worker, if configured to do so
                if self.start_worker:
                    token = current_scheduler.set(self)
                    exit_stack.callback(current_scheduler.reset, token)
                    worker = Worker(
                        self.data_store, self.event_broker, is_internal=True
                    )
                    exit_stack.enter_context(worker)

                # Signal that the scheduler has started
                self._state = RunState.started
                self.event_broker.publish_local(SchedulerStarted())
            except BaseException as exc:
                if start_future:
                    start_future.set_exception(exc)
                    return
                else:
                    raise
            else:
                if start_future:
                    start_future.set_result(None)

            exception: BaseException | None = None
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
                                    "Error computing next fire time for schedule %r of "
                                    "task %r – removing schedule",
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
                                    # Jitter must never be so high that it would cause
                                    # a fire time to equal or exceed the next fire time
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
                                    jitter = timedelta(
                                        seconds=random.uniform(0, jitter_s)
                                    )
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

                    # If we received fewer schedules than the maximum amount, sleep
                    # until the next schedule is due or the scheduler is explicitly
                    # woken up
                    wait_time = None
                    if len(schedules) < 100:
                        self._wakeup_deadline = (
                            self.data_store.get_next_schedule_run_time()
                        )
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
                        self.logger.debug(
                            "Processing more schedules on the next iteration"
                        )
            except BaseException as exc:
                exception = exc
                raise
            finally:
                self._state = RunState.stopped
                if isinstance(exception, Exception):
                    self.logger.exception("Scheduler crashed")
                elif exception:
                    self.logger.info(
                        f"Scheduler stopped due to {exception.__class__.__name__}"
                    )
                else:
                    self.logger.info("Scheduler stopped")

                self.event_broker.publish_local(SchedulerStopped(exception=exception))
