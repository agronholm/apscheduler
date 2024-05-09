from __future__ import annotations

import os
import platform
import random
import sys
from collections.abc import MutableMapping, Sequence
from contextlib import AsyncExitStack
from datetime import datetime, timedelta, timezone
from functools import partial
from inspect import isbuiltin, isclass, ismethod, ismodule
from logging import Logger, getLogger
from types import TracebackType
from typing import Any, Callable, Iterable, Literal, Mapping, cast, overload
from uuid import UUID, uuid4

import anyio
import attrs
from anyio import (
    TASK_STATUS_IGNORED,
    CancelScope,
    create_task_group,
    get_cancelled_exc_class,
    move_on_after,
    sleep,
)
from anyio.abc import TaskGroup, TaskStatus
from attr.validators import instance_of, optional

from .. import JobAdded, SerializationError, TaskLookupError
from .._context import current_async_scheduler, current_job
from .._converters import as_enum, as_timedelta
from .._enums import CoalescePolicy, ConflictPolicy, JobOutcome, RunState, SchedulerRole
from .._events import (
    Event,
    JobReleased,
    ScheduleAdded,
    SchedulerStarted,
    SchedulerStopped,
    ScheduleUpdated,
    T_Event,
)
from .._exceptions import (
    CallableLookupError,
    DeserializationError,
    JobCancelled,
    JobDeadlineMissed,
    JobLookupError,
    ScheduleLookupError,
)
from .._marshalling import callable_from_ref, callable_to_ref
from .._structures import Job, JobResult, Schedule, Task
from .._utils import UnsetValue, unset
from .._validators import non_negative_number
from ..abc import DataStore, EventBroker, JobExecutor, Subscription, Trigger
from ..datastores.memory import MemoryDataStore
from ..eventbrokers.local import LocalEventBroker
from ..executors.async_ import AsyncJobExecutor
from ..executors.subprocess import ProcessPoolJobExecutor
from ..executors.thread import ThreadPoolJobExecutor

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

_microsecond_delta = timedelta(microseconds=1)
_zero_timedelta = timedelta()

TaskType: TypeAlias = "Task | str | Callable"


@attrs.define(eq=False)
class AsyncScheduler:
    """
    An asynchronous (AnyIO based) scheduler implementation.

    Requires either :mod:`asyncio` or Trio_ to work.

    .. note:: If running on Trio, ensure that the data store and event broker are
        compatible with Trio.

    .. _AnyIO: https://pypi.org/project/anyio/
    .. _Trio: https://pypi.org/project/trio/

    :param data_store: the data store for tasks, schedules and jobs
    :param event_broker: the event broker to use for publishing an subscribing events
    :param identity: the unique identifier of the scheduler
    :param role: specifies what the scheduler should be doing when running (scheduling
        only, job running only, or both)
    :param max_concurrent_jobs: Maximum number of jobs the scheduler will run at once
    :param job_executors: a mutable mapping of executor names to executor instances
    :param default_job_executor: name of the default job executor
    :param cleanup_interval: interval (as seconds or timedelta) between automatic
        calls to :meth:`cleanup` – ``None`` to disable automatic clean-up
    :param logger: the logger instance used to log events from the scheduler, data store
        and event broker
    """

    data_store: DataStore = attrs.field(
        validator=instance_of(DataStore), factory=MemoryDataStore
    )
    event_broker: EventBroker = attrs.field(
        validator=instance_of(EventBroker), factory=LocalEventBroker
    )
    identity: str = attrs.field(kw_only=True, validator=instance_of(str), default="")
    role: SchedulerRole = attrs.field(
        kw_only=True, converter=as_enum(SchedulerRole), default=SchedulerRole.both
    )
    max_concurrent_jobs: int = attrs.field(
        kw_only=True, validator=non_negative_number, default=100
    )
    job_executors: MutableMapping[str, JobExecutor] = attrs.field(
        kw_only=True, validator=instance_of(MutableMapping), factory=dict
    )
    default_job_executor: str | None = attrs.field(
        kw_only=True, validator=optional(instance_of(str)), default=None
    )
    cleanup_interval: timedelta | None = attrs.field(
        kw_only=True,
        converter=as_timedelta,
        validator=optional(instance_of(timedelta)),
        default=timedelta(minutes=15),
    )
    logger: Logger = attrs.field(kw_only=True, default=getLogger(__name__))

    _state: RunState = attrs.field(init=False, default=RunState.stopped)
    _services_task_group: TaskGroup | None = attrs.field(init=False, default=None)
    _exit_stack: AsyncExitStack = attrs.field(init=False)
    _services_initialized: bool = attrs.field(init=False, default=False)
    _scheduler_cancel_scope: CancelScope | None = attrs.field(init=False, default=None)
    _running_jobs: set[UUID] = attrs.field(init=False, factory=set)
    _task_callables: dict[str, Callable] = attrs.field(init=False, factory=dict)

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
            valid_executors = ", ".join(self.job_executors)
            raise ValueError(
                f"default_job_executor must be one of the given job executors "
                f"({valid_executors})"
            )

    async def __aenter__(self) -> Self:
        async with AsyncExitStack() as exit_stack:
            await self._ensure_services_initialized(exit_stack)
            self._services_task_group = await exit_stack.enter_async_context(
                create_task_group()
            )
            exit_stack.callback(setattr, self, "_services_task_group", None)
            self._exit_stack = exit_stack.pop_all()

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
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

            await self.event_broker.start(exit_stack, self.logger)
            await self.data_store.start(exit_stack, self.event_broker, self.logger)

    def _check_initialized(self) -> None:
        """Raise RuntimeError if the services have not been initialized yet."""
        if not self._services_initialized:
            raise RuntimeError(
                "The scheduler has not been initialized yet. Use the scheduler as an "
                "async context manager (async with ...) in order to call methods other "
                "than run_until_stopped()."
            )

    async def _cleanup_loop(self) -> None:
        delay = self.cleanup_interval.total_seconds()
        assert delay > 0
        while self._state in (RunState.starting, RunState.started):
            await self.cleanup()
            await sleep(delay)

    @property
    def state(self) -> RunState:
        """The current running state of the scheduler."""
        return self._state

    async def cleanup(self) -> None:
        """Clean up expired job results and finished schedules."""
        await self.data_store.cleanup()
        self.logger.info("Cleaned up expired job results and finished schedules")

    @overload
    def subscribe(
        self,
        callback: Callable[[T_Event], Any],
        event_types: type[T_Event],
        *,
        one_shot: bool = ...,
        is_async: bool = ...,
    ) -> Subscription: ...

    @overload
    def subscribe(
        self,
        callback: Callable[[Event], Any],
        event_types: Iterable[type[Event]] | None = None,
        *,
        one_shot: bool = False,
        is_async: bool = True,
    ) -> Subscription: ...

    def subscribe(
        self,
        callback: Callable[[T_Event], Any],
        event_types: type[T_Event] | Iterable[type[T_Event]] | None = None,
        *,
        one_shot: bool = False,
        is_async: bool = True,
    ) -> Subscription:
        """
        Subscribe to events.

        To unsubscribe, call the :meth:`~abc.Subscription.unsubscribe` method on the
        returned object.

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

    async def configure_task(
        self,
        func_or_task_id: TaskType,
        *,
        func: Callable | UnsetValue = unset,
        job_executor: str | UnsetValue = unset,
        misfire_grace_time: float | timedelta | None | UnsetValue = unset,
        max_running_jobs: int | None | UnsetValue = unset,
    ) -> Task:
        """
        Add or update a :ref:`task` definition.

        Any options not explicitly passed to this method will use their default values
        when a new task is created:

        * ``job_executor``: the value of ``default_job_executor`` scheduler attribute
        * ``misfire_grace_time``: ``None``
        * ``max_running_jobs``: 1

        When updating a task, any options not explicitly passed will remain the same.

        If a callable is passed as the first argument, its fully qualified name will be
        used as the task ID.

        :param func_or_task_id: either a task, task ID or a callable
        :param func: a callable that will be associated with the task (can be omitted if
            the callable is already passed as ``func_or_task_id``)
        :param job_executor: name of the job executor to run the task with
        :param misfire_grace_time: maximum number of seconds the scheduled job's actual
            run time is allowed to be late, compared to the scheduled run time
        :param max_running_jobs: maximum number of instances of the task that are
            allowed to run concurrently
        :raises TypeError: if ``func_or_task_id`` is neither a task, task ID or a
            callable
        :return: the created or updated task definition

        """
        func_ref: str | None = None
        if callable(func_or_task_id):
            task_ref = callable_to_ref(func_or_task_id)
            if func is unset:
                func = func_or_task_id
        elif isinstance(func_or_task_id, Task):
            task_ref = func_or_task_id.id
        elif isinstance(func_or_task_id, str) and func_or_task_id:
            task_ref = func_or_task_id
        else:
            raise TypeError(
                "func_or_task_id must be either a task, its identifier or a callable"
            )

        if callable(func):
            self._task_callables[task_ref] = func
            try:
                func_ref = callable_to_ref(func)
            except SerializationError:
                pass

        modified = False
        try:
            task = await self.data_store.get_task(task_ref)
        except TaskLookupError:
            task = Task(
                id=task_ref,
                func=func_ref,
                job_executor=(
                    self.default_job_executor if job_executor is unset else job_executor
                ),
                max_running_jobs=(
                    None if max_running_jobs is unset else max_running_jobs
                ),
                misfire_grace_time=(
                    None if misfire_grace_time is unset else misfire_grace_time
                ),
            )
            modified = True
        else:
            if func is not unset and task.func != func_ref:
                task.func = func_ref
                modified = True

            if job_executor is not unset and task.job_executor != job_executor:
                task.job_executor = job_executor
                modified = True

            if (
                max_running_jobs is not unset
                and task.max_running_jobs != max_running_jobs
            ):
                task.max_running_jobs = max_running_jobs
                modified = True

            if (
                misfire_grace_time is not unset
                and task.misfire_grace_time != misfire_grace_time
            ):
                task.misfire_grace_time = misfire_grace_time
                modified = True

        if modified:
            await self.data_store.add_task(task)

        return task

    async def get_tasks(self) -> Sequence[Task]:
        """
        Retrieve all currently defined tasks.

        :return: a sequence of tasks, sorted by ID

        """
        self._check_initialized()
        return await self.data_store.get_tasks()

    async def add_schedule(
        self,
        func_or_task_id: TaskType,
        trigger: Trigger,
        *,
        id: str | None = None,
        args: Iterable | None = None,
        kwargs: Mapping[str, Any] | None = None,
        paused: bool = False,
        job_executor: str | UnsetValue = unset,
        coalesce: CoalescePolicy = CoalescePolicy.latest,
        misfire_grace_time: float | timedelta | None | UnsetValue = unset,
        max_jitter: float | timedelta | None = None,
        max_running_jobs: int | None | UnsetValue = unset,
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
        :param paused: whether the schedule is paused
        :param job_executor: name of the job executor to run the task with
        :param coalesce: determines what to do when processing the schedule if multiple
            fire times have become due for this schedule since the last processing
        :param misfire_grace_time: maximum number of seconds the scheduled job's actual
            run time is allowed to be late, compared to the scheduled run time
        :param max_jitter: maximum number of seconds to randomly add to the scheduled
            time for each job created from this schedule
        :param max_running_jobs: maximum number of instances of the task that are
            allowed to run concurrently (if not set, uses the default misfire grace time
            from the associated task, or ``None`` if there is no existing task)
        :param conflict_policy: determines what to do if a schedule with the same ID
            already exists in the data store
        :return: the ID of the newly added schedule

        """
        self._check_initialized()
        schedule_id = id or str(uuid4())
        args = tuple(args or ())
        kwargs = dict(kwargs or {})

        # Unpack the function and positional + keyword arguments from a partial()
        if isinstance(func_or_task_id, partial):
            args = func_or_task_id.args + args
            kwargs.update(func_or_task_id.keywords)
            func_or_task_id = func_or_task_id.func

        # For instance methods, use the unbound function as the function, and  the
        # "self" argument as the first positional argument
        if ismethod(func_or_task_id):
            args = (func_or_task_id.__self__, *args)
            func_or_task_id = func_or_task_id.__func__
        elif (
            isbuiltin(func_or_task_id)
            and func_or_task_id.__self__ is not None
            and not ismodule(func_or_task_id.__self__)
        ):
            args = (func_or_task_id.__self__, *args)
            method_class = type(func_or_task_id.__self__)
            func_or_task_id = getattr(method_class, func_or_task_id.__name__)

        task = await self.configure_task(
            func_or_task_id,
            job_executor=job_executor,
            max_running_jobs=max_running_jobs,
        )
        schedule = Schedule(
            id=schedule_id,
            task_id=task.id,
            trigger=trigger,
            args=args,
            kwargs=kwargs,
            paused=paused,
            coalesce=coalesce,
            misfire_grace_time=task.misfire_grace_time
            if misfire_grace_time is unset
            else misfire_grace_time,
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

    async def pause_schedule(self, id: str) -> None:
        """Pause the specified schedule."""
        self._check_initialized()
        await self.data_store.add_schedule(
            schedule=attrs.evolve(await self.get_schedule(id), paused=True),
            conflict_policy=ConflictPolicy.replace,
        )

    async def unpause_schedule(
        self,
        id: str,
        *,
        resume_from: datetime | Literal["now"] | None = None,
    ) -> None:
        """
        Unpause the specified schedule.


        :param resume_from: the time to resume the schedules from, or ``'now'`` as a
            shorthand for ``datetime.now(tz=UTC)`` or ``None`` to resume from where the
            schedule left off which may cause it to misfire

        """
        self._check_initialized()
        schedule = await self.get_schedule(id)

        if resume_from == "now":
            resume_from = datetime.now(tz=timezone.utc)

        if resume_from is None:
            next_fire_time = schedule.next_fire_time
        elif (
            schedule.next_fire_time is not None
            and schedule.next_fire_time >= resume_from
        ):
            next_fire_time = schedule.next_fire_time
        else:
            # Advance `next_fire_time` until its at or past `resume_from`, or until it's
            # exhausted
            while next_fire_time := schedule.trigger.next():
                if next_fire_time is None or next_fire_time >= resume_from:
                    break

        await self.data_store.add_schedule(
            schedule=attrs.evolve(
                schedule,
                paused=False,
                next_fire_time=next_fire_time,
            ),
            conflict_policy=ConflictPolicy.replace,
        )

    async def add_job(
        self,
        func_or_task_id: TaskType,
        *,
        args: Iterable | None = None,
        kwargs: Mapping[str, Any] | None = None,
        job_executor: str | None | UnsetValue = unset,
        result_expiration_time: timedelta | float = 0,
    ) -> UUID:
        """
        Add a job to the data store.

        :param func_or_task_id:
            Either the ID of a pre-existing task, or a function/method. If a function is
            given, a task will be created with the fully qualified name of the function
            as the task ID (unless that task already exists of course).
        :param args: positional arguments to call the target callable with
        :param kwargs: keyword arguments to call the target callable with
        :param job_executor: name of the job executor to run the job with
        :param result_expiration_time: the minimum time (as seconds, or timedelta) to
            keep the result of the job available for fetching (the result won't be
            saved at all if that time is 0)
        :return: the ID of the newly created job

        """
        self._check_initialized()
        args = tuple(args or ())
        kwargs = dict(kwargs or {})

        # Unpack the function and positional + keyword arguments from a partial()
        if isinstance(func_or_task_id, partial):
            args = func_or_task_id.args + args
            kwargs.update(func_or_task_id.keywords)
            func_or_task_id = func_or_task_id.func

        # For instance methods, use the unbound function as the function, and  the
        # "self" argument as the first positional argument
        if ismethod(func_or_task_id):
            args = (func_or_task_id.__self__, *args)
            func_or_task_id = func_or_task_id.__func__
        elif (
            isbuiltin(func_or_task_id)
            and func_or_task_id.__self__ is not None
            and not ismodule(func_or_task_id.__self__)
        ):
            args = (func_or_task_id.__self__, *args)
            method_class = type(func_or_task_id.__self__)
            func_or_task_id = getattr(method_class, func_or_task_id.__name__)

        task = await self.configure_task(func_or_task_id, job_executor=job_executor)
        job = Job(
            task_id=task.id,
            args=args or (),
            kwargs=kwargs or {},
            result_expiration_time=result_expiration_time,
        )
        await self.data_store.add_job(job)
        return job.id

    async def get_jobs(self) -> Sequence[Job]:
        """Retrieve all jobs from the data store."""
        self._check_initialized()
        return await self.data_store.get_jobs()

    async def get_job_result(self, job_id: UUID, *, wait: bool = True) -> JobResult:
        """
        Retrieve the result of a job.

        :param job_id: the ID of the job
        :param wait: if ``True``, wait until the job has ended (one way or another),
            ``False`` to raise an exception if the result is not yet available
        :raises JobLookupError: if neither the job or its result exist in the data
            store, or the job exists but the result is not ready yet and ``wait=False``
            is set

        """
        self._check_initialized()
        wait_event = anyio.Event()

        def listener(event: JobReleased) -> None:
            if event.job_id == job_id:
                wait_event.set()

        with self.event_broker.subscribe(listener, {JobReleased}):
            job_exists = bool(await self.data_store.get_jobs([job_id]))
            if result := await self.data_store.get_job_result(job_id):
                return result

            if job_exists and wait:
                await wait_event.wait()
            else:
                raise JobLookupError(job_id)

        return await self.data_store.get_job_result(job_id)

    async def run_job(
        self,
        func_or_task_id: str | Callable,
        *,
        args: Iterable | None = None,
        kwargs: Mapping[str, Any] | None = None,
        job_executor: str | UnsetValue = unset,
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
                job_executor=self.default_job_executor
                if job_executor is unset
                else job_executor,
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
        Wait until the scheduler is in the :attr:`~RunState.stopped` or
        :attr:`~RunState.stopping` state.

        If the scheduler is already stopped or in the process of stopping, this method
        returns immediately. Otherwise, it waits until the scheduler posts the
        :class:`SchedulerStopped` event.

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

                    # Start periodic cleanups
                    if self.cleanup_interval:
                        task_group.start_soon(self._cleanup_loop)
                        self.logger.debug(
                            "Started internal cleanup loop with interval: %s",
                            self.cleanup_interval,
                        )

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
                                max_jitter = min(
                                    [
                                        max_jitter,
                                        (
                                            next_fire_time
                                            - fire_time
                                            - _microsecond_delta
                                        ).total_seconds(),
                                    ]
                                )

                            jitter = timedelta(seconds=random.uniform(0, max_jitter))
                            fire_time += jitter

                        if schedule.misfire_grace_time is None:
                            start_deadline = None
                        else:
                            start_deadline = fire_time + schedule.misfire_grace_time

                        schedule.last_fire_time = fire_time
                        job = Job(
                            task_id=schedule.task_id,
                            args=schedule.args,
                            kwargs=schedule.kwargs,
                            schedule_id=schedule.id,
                            scheduled_fire_time=fire_time,
                            jitter=jitter,
                            start_deadline=start_deadline,
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

    def _get_task_callable(self, task: Task) -> Callable:
        try:
            return self._task_callables[task.id]
        except KeyError:
            if task.func:
                try:
                    func = self._task_callables[task.id] = callable_from_ref(task.func)
                except DeserializationError as exc:
                    raise CallableLookupError(
                        f"Error looking up the callable ({task.func!r}) for task "
                        f"{task.id!r}"
                    ) from exc

                return func

            raise CallableLookupError(
                f"Task {task.id} requires a locally defined callable to be run, but no "
                f"such callable has been defined. Call "
                f"scheduler.configure_task({task.id!r}, func=...) to define the local "
                f"callable."
            )

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
                        func = self._get_task_callable(task)
                        self._running_jobs.add(job.id)
                        task_group.start_soon(
                            self._run_job, job, func, task.job_executor
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
                await self.data_store.release_job(self.identity, job, result)
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
                    await self.data_store.release_job(self.identity, job, result)
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
                await self.data_store.release_job(self.identity, job, result)
                if not isinstance(exc, Exception):
                    raise
            else:
                self.logger.info("Job %s completed successfully", job.id)
                result = JobResult.from_job(
                    job,
                    JobOutcome.success,
                    return_value=retval,
                )
                await self.data_store.release_job(self.identity, job, result)
            finally:
                current_job.reset(token)
        finally:
            self._running_jobs.remove(job.id)
