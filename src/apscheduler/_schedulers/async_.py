from __future__ import annotations

import os
import platform
import random
import sys
from collections.abc import MutableMapping
from contextlib import AsyncExitStack
from datetime import datetime, timedelta, timezone
from inspect import isclass
from logging import Logger, getLogger
from types import TracebackType
from typing import Any, Callable, Iterable, Mapping, cast
from uuid import UUID, uuid4

import anyio
import attrs
from anyio import (
    TASK_STATUS_IGNORED,
    CancelScope,
    create_task_group,
    get_cancelled_exc_class,
    move_on_after,
)
from anyio.abc import TaskGroup, TaskStatus
from attr.validators import instance_of

from .. import JobAdded
from .._context import current_async_scheduler, current_job
from .._enums import CoalescePolicy, ConflictPolicy, JobOutcome, RunState, SchedulerRole
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
from .._validators import non_negative_number
from ..abc import DataStore, EventBroker, JobExecutor, Subscription, Trigger
from ..datastores.memory import MemoryDataStore
from ..eventbrokers.local import LocalEventBroker
from ..executors.async_ import AsyncJobExecutor
from ..executors.subprocess import ProcessPoolJobExecutor
from ..executors.thread import ThreadPoolJobExecutor
from ..marshalling import callable_to_ref

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

_microsecond_delta = timedelta(microseconds=1)
_zero_timedelta = timedelta()


@attrs.define(eq=False)
class AsyncScheduler:
    """
    An asynchronous (AnyIO based) scheduler implementation.

    :param data_store: the data store for tasks, schedules and jobs
    :param event_broker: the event broker to use for publishing an subscribing events
    :param max_concurrent_jobs: Maximum number of jobs the worker will run at once
    :param role: specifies what the scheduler should be doing when running
    :param process_schedules: ``True`` to process due schedules in this scheduler
    """

    data_store: DataStore = attrs.field(
        validator=instance_of(DataStore), factory=MemoryDataStore
    )
    event_broker: EventBroker = attrs.field(
        validator=instance_of(EventBroker), factory=LocalEventBroker
    )
    identity: str = attrs.field(kw_only=True, default=None)
    role: SchedulerRole = attrs.field(kw_only=True, default=SchedulerRole.both)
    max_concurrent_jobs: int = attrs.field(
        kw_only=True, validator=non_negative_number, default=100
    )
    job_executors: MutableMapping[str, JobExecutor] | None = attrs.field(
        kw_only=True, default=None
    )
    default_job_executor: str | None = attrs.field(kw_only=True, default=None)
    logger: Logger = attrs.field(kw_only=True, default=getLogger(__name__))

    _state: RunState = attrs.field(init=False, default=RunState.stopped)
    _services_task_group: TaskGroup | None = attrs.field(init=False, default=None)
    _exit_stack: AsyncExitStack = attrs.field(init=False, factory=AsyncExitStack)
    _services_initialized: bool = attrs.field(init=False, default=False)
    _scheduler_cancel_scope: CancelScope | None = attrs.field(init=False, default=None)
    _running_jobs: set[Job] = attrs.field(init=False, factory=set)

    def __attrs_post_init__(self) -> None:
        if not self.identity:
            self.identity = f"{platform.node()}-{os.getpid()}-{id(self)}"

        if not self.job_executors:
            self.job_executors = {
                "async": AsyncJobExecutor(),
                "threadpool": ThreadPoolJobExecutor(),
                "processpool": ProcessPoolJobExecutor(),
            }

        if not self.default_job_executor:
            self.default_job_executor = next(iter(self.job_executors))
        elif self.default_job_executor not in self.job_executors:
            raise ValueError(
                "default_job_executor must be one of the given job executors"
            )

    async def __aenter__(self: Self) -> Self:
        await self._exit_stack.__aenter__()
        try:
            await self._ensure_services_initialized(self._exit_stack)
            self._services_task_group = await self._exit_stack.enter_async_context(
                create_task_group()
            )
            self._exit_stack.callback(setattr, self, "_services_task_group", None)
        except BaseException as exc:
            await self._exit_stack.__aexit__(type(exc), exc, exc.__traceback__)
            raise

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ) -> None:
        await self.stop()
        await self._exit_stack.__aexit__(exc_type, exc_val, exc_tb)

    async def _ensure_services_initialized(self, exit_stack: AsyncExitStack) -> None:
        """
        Initialize the data store and event broker if this hasn't already been done.

        """
        if not self._services_initialized:
            self._services_initialized = True
            exit_stack.callback(setattr, self, "_services_initialized", False)

            await self.event_broker.start(exit_stack)
            await self.data_store.start(exit_stack, self.event_broker)

    def _check_initialized(self) -> None:
        """Raise RuntimeError if the services have not been initialized yet."""
        if not self._services_initialized:
            raise RuntimeError(
                "The scheduler has not been initialized yet. Use the scheduler as an "
                "async context manager (async with ...) in order to call methods other "
                "than run_until_complete()."
            )

    @property
    def state(self) -> RunState:
        """The current running state of the scheduler."""
        return self._state

    def subscribe(
        self,
        callback: Callable[[Event], Any],
        event_types: type[Event] | Iterable[type[Event]] | None = None,
        *,
        one_shot: bool = False,
        is_async: bool = True,
    ) -> Subscription:
        """
        Subscribe to events.

        To unsubscribe, call the :meth:`Subscription.unsubscribe` method on the returned
        object.

        :param callback: callable to be called with the event object when an event is
            published
        :param event_types: an event class or an iterable event classes to subscribe to
        :param one_shot: if ``True``, automatically unsubscribe after the first matching
            event
        :param is_async: ``True`` if the (synchronous) callback should be called on the
            event loop thread, ``False`` if it should be called in a worker thread.
            If ``callback`` is a coroutine function, this flag is ignored.

        """
        self._check_initialized()
        if isclass(event_types):
            event_types = {event_types}

        return self.event_broker.subscribe(
            callback, event_types, is_async=is_async, one_shot=one_shot
        )

    async def get_next_event(
        self, event_types: type[Event] | Iterable[type[Event]]
    ) -> Event:
        """
        Wait until the next event matching one of the given types arrives.

        :param event_types: an event class or an iterable event classes to subscribe to

        """
        received_event: Event | None = None

        def receive_event(ev: Event) -> None:
            nonlocal received_event
            received_event = ev
            event.set()

        event = anyio.Event()
        with self.subscribe(receive_event, event_types, one_shot=True):
            await event.wait()
            return received_event

    async def add_schedule(
        self,
        func_or_task_id: str | Callable,
        trigger: Trigger,
        *,
        id: str | None = None,
        args: Iterable | None = None,
        kwargs: Mapping[str, Any] | None = None,
        job_executor: str | None = None,
        coalesce: CoalescePolicy = CoalescePolicy.latest,
        misfire_grace_time: float | timedelta | None = None,
        max_jitter: float | timedelta | None = None,
        max_running_jobs: int | None = None,
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
        :param job_executor: name of the job executor to run the task with
        :param coalesce: determines what to do when processing the schedule if multiple
            fire times have become due for this schedule since the last processing
        :param misfire_grace_time: maximum number of seconds the scheduled job's actual
            run time is allowed to be late, compared to the scheduled run time
        :param max_jitter: maximum number of seconds to randomly add to the scheduled
            time for each job created from this schedule
        :param max_running_jobs: maximum number of instances of the task that are
            allowed to run concurrently
        :param conflict_policy: determines what to do if a schedule with the same ID
            already exists in the data store
        :return: the ID of the newly added schedule

        """
        self._check_initialized()
        id = id or str(uuid4())
        args = tuple(args or ())
        kwargs = dict(kwargs or {})
        if isinstance(misfire_grace_time, (int, float)):
            misfire_grace_time = timedelta(seconds=misfire_grace_time)

        if callable(func_or_task_id):
            task = Task(
                id=callable_to_ref(func_or_task_id),
                func=func_or_task_id,
                executor=job_executor or self.default_job_executor,
                max_running_jobs=max_running_jobs,
            )
            await self.data_store.add_task(task)
        else:
            task = await self.data_store.get_task(func_or_task_id)

        schedule = Schedule(
            id=id,
            task_id=task.id,
            trigger=trigger,
            args=args,
            kwargs=kwargs,
            coalesce=coalesce,
            misfire_grace_time=misfire_grace_time,
            max_jitter=max_jitter,
        )
        schedule.next_fire_time = trigger.next()
        await self.data_store.add_schedule(schedule, conflict_policy)
        self.logger.info(
            "Added new schedule (task=%r, trigger=%r); next run time at %s",
            task,
            trigger,
            schedule.next_fire_time,
        )
        return schedule.id

    async def get_schedule(self, id: str) -> Schedule:
        """
        Retrieve a schedule from the data store.

        :param id: the unique identifier of the schedule
        :raises ScheduleLookupError: if the schedule could not be found

        """
        self._check_initialized()
        schedules = await self.data_store.get_schedules({id})
        if schedules:
            return schedules[0]
        else:
            raise ScheduleLookupError(id)

    async def get_schedules(self) -> list[Schedule]:
        """
        Retrieve all schedules from the data store.

        :return: a list of schedules, in an unspecified order

        """
        self._check_initialized()
        return await self.data_store.get_schedules()

    async def remove_schedule(self, id: str) -> None:
        """
        Remove the given schedule from the data store.

        :param id: the unique identifier of the schedule

        """
        self._check_initialized()
        await self.data_store.remove_schedules({id})

    async def add_job(
        self,
        func_or_task_id: str | Callable,
        *,
        args: Iterable | None = None,
        kwargs: Mapping[str, Any] | None = None,
        job_executor: str | None = None,
        result_expiration_time: timedelta | float = 0,
    ) -> UUID:
        """
        Add a job to the data store.

        :param func_or_task_id:
        :param job_executor: name of the job executor to run the task with
        :param args: positional arguments to call the target callable with
        :param kwargs: keyword arguments to call the target callable with
        :param job_executor: name of the job executor to run the task with
        :param result_expiration_time: the minimum time (as seconds, or timedelta) to
            keep the result of the job available for fetching (the result won't be
            saved at all if that time is 0)
        :return: the ID of the newly created job

        """
        self._check_initialized()
        if callable(func_or_task_id):
            task = Task(
                id=callable_to_ref(func_or_task_id),
                func=func_or_task_id,
                executor=job_executor or self.default_job_executor,
            )
            await self.data_store.add_task(task)
        else:
            task = await self.data_store.get_task(func_or_task_id)

        job = Job(
            task_id=task.id,
            args=args or (),
            kwargs=kwargs or {},
            result_expiration_time=result_expiration_time,
        )
        await self.data_store.add_job(job)
        return job.id

    async def get_job_result(self, job_id: UUID, *, wait: bool = True) -> JobResult:
        """
        Retrieve the result of a job.

        :param job_id: the ID of the job
        :param wait: if ``True``, wait until the job has ended (one way or another),
            ``False`` to raise an exception if the result is not yet available
        :raises JobLookupError: if ``wait=False`` and the job result does not exist in
            the data store

        """
        self._check_initialized()
        wait_event = anyio.Event()

        def listener(event: JobReleased) -> None:
            if event.job_id == job_id:
                wait_event.set()

        with self.event_broker.subscribe(listener, {JobReleased}):
            result = await self.data_store.get_job_result(job_id)
            if result:
                return result
            elif not wait:
                raise JobLookupError(job_id)

            await wait_event.wait()

        return await self.data_store.get_job_result(job_id)

    async def run_job(
        self,
        func_or_task_id: str | Callable,
        *,
        args: Iterable | None = None,
        kwargs: Mapping[str, Any] | None = None,
        job_executor: str | None = None,
    ) -> Any:
        """
        Convenience method to add a job and then return its result.

        If the job raised an exception, that exception will be reraised here.

        :param func_or_task_id: either a callable or an ID of an existing task
            definition
        :param args: positional arguments to be passed to the task function
        :param kwargs: keyword arguments to be passed to the task function
        :param job_executor: name of the job executor to run the task with
        :returns: the return value of the task function

        """
        self._check_initialized()
        job_complete_event = anyio.Event()

        def listener(event: JobReleased) -> None:
            if event.job_id == job_id:
                job_complete_event.set()

        job_id: UUID | None = None
        with self.event_broker.subscribe(listener, {JobReleased}):
            job_id = await self.add_job(
                func_or_task_id,
                args=args,
                kwargs=kwargs,
                job_executor=job_executor,
                result_expiration_time=timedelta(minutes=15),
            )
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
            raise RuntimeError(f"Unknown job outcome: {result.outcome}")

    async def stop(self) -> None:
        """
        Signal the scheduler that it should stop processing schedules.

        This method does not wait for the scheduler to actually stop.
        For that, see :meth:`wait_until_stopped`.

        """
        if self._state is RunState.started and self._scheduler_cancel_scope:
            self._state = RunState.stopping
            self._scheduler_cancel_scope.cancel()

    async def wait_until_stopped(self) -> None:
        """
        Wait until the scheduler is in the "stopped" or "stopping" state.

        If the scheduler is already stopped or in the process of stopping, this method
        returns immediately. Otherwise, it waits until the scheduler posts the
        ``SchedulerStopped`` event.

        """
        if self._state not in (RunState.stopped, RunState.stopping):
            await self.get_next_event(SchedulerStopped)

    async def start_in_background(self) -> None:
        self._check_initialized()
        await self._services_task_group.start(self.run_until_stopped)

    async def run_until_stopped(
        self, *, task_status: TaskStatus = TASK_STATUS_IGNORED
    ) -> None:
        """Run the scheduler until explicitly stopped."""
        if self._state is not RunState.stopped:
            raise RuntimeError(
                f'Cannot start the scheduler when it is in the "{self._state}" '
                f"state"
            )

        self._state = RunState.starting
        async with AsyncExitStack() as exit_stack:
            await self._ensure_services_initialized(exit_stack)

            # Set this scheduler as the current scheduler
            token = current_async_scheduler.set(self)
            exit_stack.callback(current_async_scheduler.reset, token)

            exception: BaseException | None = None
            try:
                async with create_task_group() as task_group:
                    self._scheduler_cancel_scope = task_group.cancel_scope
                    exit_stack.callback(setattr, self, "_scheduler_cancel_scope", None)

                    # Start processing due schedules, if configured to do so
                    if self.role in (SchedulerRole.scheduler, SchedulerRole.both):
                        await task_group.start(self._process_schedules)

                    # Start processing due jobs, if configured to do so
                    if self.role in (SchedulerRole.worker, SchedulerRole.both):
                        await task_group.start(self._process_jobs)

                    # Signal that the scheduler has started
                    self._state = RunState.started
                    self.logger.info("Scheduler started")
                    task_status.started()
                    await self.event_broker.publish_local(SchedulerStarted())
            except BaseException as exc:
                exception = exc
                raise
            finally:
                self._state = RunState.stopped

                if not exception or isinstance(exception, get_cancelled_exc_class()):
                    self.logger.info("Scheduler stopped")
                elif isinstance(exception, Exception):
                    self.logger.exception("Scheduler crashed")
                elif exception:
                    self.logger.info(
                        "Scheduler stopped due to %s", exception.__class__.__name__
                    )

                with move_on_after(3, shield=True):
                    await self.event_broker.publish_local(
                        SchedulerStopped(exception=exception)
                    )

    async def _process_schedules(self, *, task_status: TaskStatus) -> None:
        wakeup_event = anyio.Event()
        wakeup_deadline: datetime | None = None

        async def schedule_added_or_modified(event: Event) -> None:
            event_ = cast("ScheduleAdded | ScheduleUpdated", event)
            if not wakeup_deadline or (
                event_.next_fire_time and event_.next_fire_time < wakeup_deadline
            ):
                self.logger.debug(
                    "Detected a %s event – waking up the scheduler to process "
                    "schedules",
                    type(event).__name__,
                )
                wakeup_event.set()

        subscription = self.event_broker.subscribe(
            schedule_added_or_modified, {ScheduleAdded, ScheduleUpdated}
        )
        with subscription:
            # Signal that we are ready, and wait for the scheduler start event
            task_status.started()
            await self.get_next_event(SchedulerStarted)

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
                                # Jitter must never be so high that it would cause a
                                # fire time to equal or exceed the next fire time
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
                        )
                        await self.data_store.add_job(job)

                # Update the schedules (and release the scheduler's claim on them)
                await self.data_store.release_schedules(self.identity, schedules)

                # If we received fewer schedules than the maximum amount, sleep
                # until the next schedule is due or the scheduler is explicitly
                # woken up
                wait_time = None
                if len(schedules) < 100:
                    wakeup_deadline = await self.data_store.get_next_schedule_run_time()
                    if wakeup_deadline:
                        wait_time = (
                            wakeup_deadline - datetime.now(timezone.utc)
                        ).total_seconds()
                        self.logger.debug(
                            "Sleeping %.3f seconds until the next fire time (%s)",
                            wait_time,
                            wakeup_deadline,
                        )
                    else:
                        self.logger.debug("Waiting for any due schedules to appear")

                    with move_on_after(wait_time):
                        await wakeup_event.wait()
                        wakeup_event = anyio.Event()
                else:
                    self.logger.debug("Processing more schedules on the next iteration")

    async def _process_jobs(self, *, task_status: TaskStatus) -> None:
        wakeup_event = anyio.Event()

        async def job_added(event: Event) -> None:
            if len(self._running_jobs) < self.max_concurrent_jobs:
                wakeup_event.set()

        async with AsyncExitStack() as exit_stack:
            # Start the job executors
            for job_executor in self.job_executors.values():
                await job_executor.start(exit_stack)

            task_group = await exit_stack.enter_async_context(create_task_group())

            # Fetch new jobs every time
            exit_stack.enter_context(self.event_broker.subscribe(job_added, {JobAdded}))

            # Signal that we are ready, and wait for the scheduler start event
            task_status.started()
            await self.get_next_event(SchedulerStarted)

            while self._state is RunState.started:
                limit = self.max_concurrent_jobs - len(self._running_jobs)
                if limit > 0:
                    jobs = await self.data_store.acquire_jobs(self.identity, limit)
                    for job in jobs:
                        task = await self.data_store.get_task(job.task_id)
                        self._running_jobs.add(job.id)
                        task_group.start_soon(
                            self._run_job, job, task.func, task.executor
                        )

                await wakeup_event.wait()
                wakeup_event = anyio.Event()

    async def _run_job(self, job: Job, func: Callable, executor: str) -> None:
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

            try:
                job_executor = self.job_executors[executor]
            except KeyError:
                return

            token = current_job.set(job)
            try:
                retval = await job_executor.run_job(func, job)
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
