from __future__ import annotations

import atexit
import sys
import threading
from collections.abc import Iterable, Mapping, MutableMapping, Sequence
from contextlib import ExitStack
from datetime import datetime, timedelta
from functools import partial
from logging import Logger
from types import TracebackType
from typing import Any, Callable, Literal, overload
from uuid import UUID

import attrs
from anyio.from_thread import BlockingPortal, start_blocking_portal

from .. import current_scheduler
from .._enums import CoalescePolicy, ConflictPolicy, RunState, SchedulerRole
from .._events import Event, T_Event
from .._structures import Job, JobResult, MetadataType, Schedule, Task, TaskDefaults
from .._utils import UnsetValue, create_repr, unset
from ..abc import DataStore, EventBroker, JobExecutor, Subscription, Trigger
from .async_ import AsyncScheduler, TaskType

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


@attrs.define(init=False, repr=False)
class Scheduler:
    """
    A synchronous wrapper for :class:`AsyncScheduler`.

    When started, this wrapper launches an asynchronous event loop in a separate thread
    and runs the asynchronous scheduler there. This thread is shut down along with the
    scheduler.

    See the documentation of the :class:`AsyncScheduler` class for the documentation of
    the configuration options.
    """

    _async_scheduler: AsyncScheduler
    _exit_stack: ExitStack = attrs.field(init=False, factory=ExitStack)
    _portal: BlockingPortal | None = attrs.field(init=False, default=None)
    _lock: threading.Lock = attrs.field(init=False, factory=threading.Lock)

    def __init__(
        self,
        data_store: DataStore | None = None,
        event_broker: EventBroker | None = None,
        *,
        identity: str = "",
        role: SchedulerRole = SchedulerRole.both,
        max_concurrent_jobs: int = 100,
        cleanup_interval: float | timedelta | None = None,
        lease_duration: timedelta = timedelta(seconds=30),
        job_executors: MutableMapping[str, JobExecutor] | None = None,
        task_defaults: TaskDefaults | None = None,
        logger: Logger | None = None,
    ):
        kwargs: dict[str, Any] = {}
        if data_store is not None:
            kwargs["data_store"] = data_store

        if event_broker is not None:
            kwargs["event_broker"] = event_broker

        if logger is not None:
            kwargs["logger"] = logger

        if task_defaults is None:
            task_defaults = TaskDefaults()

        if task_defaults.job_executor is unset:
            task_defaults.job_executor = "threadpool"

        async_scheduler = AsyncScheduler(
            identity=identity,
            role=role,
            task_defaults=task_defaults,
            max_concurrent_jobs=max_concurrent_jobs,
            job_executors=job_executors or {},
            cleanup_interval=cleanup_interval,
            lease_duration=lease_duration,
            **kwargs,
        )
        self.__attrs_init__(async_scheduler=async_scheduler)

    @property
    def logger(self) -> Logger:
        return self._async_scheduler.logger

    @property
    def data_store(self) -> DataStore:
        return self._async_scheduler.data_store

    @property
    def event_broker(self) -> EventBroker:
        return self._async_scheduler.event_broker

    @property
    def identity(self) -> str:
        return self._async_scheduler.identity

    @property
    def role(self) -> SchedulerRole:
        return self._async_scheduler.role

    @property
    def max_concurrent_jobs(self) -> int:
        return self._async_scheduler.max_concurrent_jobs

    @property
    def cleanup_interval(self) -> timedelta | None:
        return self._async_scheduler.cleanup_interval

    @property
    def lease_duration(self) -> timedelta:
        return self._async_scheduler.lease_duration

    @property
    def job_executors(self) -> MutableMapping[str, JobExecutor]:
        return self._async_scheduler.job_executors

    @property
    def task_defaults(self) -> TaskDefaults:
        return self._async_scheduler.task_defaults

    @property
    def state(self) -> RunState:
        """The current running state of the scheduler."""
        return self._async_scheduler.state

    def __enter__(self: Self) -> Self:
        self._ensure_services_ready(self._exit_stack)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self._exit_stack.__exit__(exc_type, exc_val, exc_tb)

    def _ensure_services_ready(
        self, exit_stack: ExitStack | None = None
    ) -> BlockingPortal:
        """Ensure that the underlying asynchronous scheduler has been initialized."""
        with self._lock:
            if self._portal is None:
                if exit_stack is None:
                    self._exit_stack = exit_stack = ExitStack()
                    atexit.register(self._exit_stack.close)
                else:
                    exit_stack = self._exit_stack

                # Set this scheduler as the current synchronous scheduler
                token = current_scheduler.set(self)
                exit_stack.callback(current_scheduler.reset, token)

                self._portal = exit_stack.enter_context(start_blocking_portal())
                exit_stack.callback(setattr, self, "_portal", None)
                exit_stack.enter_context(
                    self._portal.wrap_async_context_manager(self._async_scheduler)
                )

        return self._portal

    def __repr__(self) -> str:
        return create_repr(self, "identity", "role", "data_store", "event_broker")

    def cleanup(self) -> None:
        portal = self._ensure_services_ready()
        return portal.call(self._async_scheduler.cleanup)

    @overload
    def subscribe(
        self,
        callback: Callable[[T_Event], Any],
        event_types: type[T_Event],
        *,
        one_shot: bool = ...,
    ) -> Subscription: ...

    @overload
    def subscribe(
        self,
        callback: Callable[[Event], Any],
        event_types: Iterable[type[Event]] | None = None,
        *,
        one_shot: bool = False,
    ) -> Subscription: ...

    def subscribe(
        self,
        callback: Callable[[T_Event], Any],
        event_types: type[T_Event] | Iterable[type[T_Event]] | None = None,
        *,
        one_shot: bool = False,
    ) -> Subscription:
        """
        Subscribe to events.

        To unsubscribe, call the :meth:`~abc.Subscription.unsubscribe` method on the
        returned object.

        :param callback: callable to be called with the event object when an event is
            published
        :param event_types: an iterable of concrete Event classes to subscribe to
        :param one_shot: if ``True``, automatically unsubscribe after the first matching
            event

        """
        portal = self._ensure_services_ready()
        return portal.call(
            partial(
                self._async_scheduler.subscribe,
                callback,
                event_types,
                is_async=False,
                one_shot=one_shot,
            )
        )

    @overload
    def get_next_event(self, event_types: type[T_Event]) -> T_Event: ...

    @overload
    def get_next_event(self, event_types: Iterable[type[Event]]) -> Event: ...

    def get_next_event(self, event_types: type[Event] | Iterable[type[Event]]) -> Event:
        portal = self._ensure_services_ready()
        return portal.call(partial(self._async_scheduler.get_next_event, event_types))

    def configure_task(
        self,
        func_or_task_id: TaskType,
        *,
        func: Callable[..., Any] | UnsetValue = unset,
        job_executor: str | UnsetValue = unset,
        misfire_grace_time: float | timedelta | None | UnsetValue = unset,
        max_running_jobs: int | None | UnsetValue = unset,
        metadata: MetadataType | UnsetValue = unset,
    ) -> Task:
        portal = self._ensure_services_ready()
        return portal.call(
            partial(
                self._async_scheduler.configure_task,
                func_or_task_id,
                func=func,
                job_executor=job_executor,
                misfire_grace_time=misfire_grace_time,
                max_running_jobs=max_running_jobs,
                metadata=metadata,
            )
        )

    def get_tasks(self) -> Sequence[Task]:
        portal = self._ensure_services_ready()
        return portal.call(self._async_scheduler.get_tasks)

    def add_schedule(
        self,
        func_or_task_id: TaskType,
        trigger: Trigger,
        *,
        id: str | None = None,
        args: Iterable[Any] | None = None,
        kwargs: Mapping[str, Any] | None = None,
        paused: bool = False,
        coalesce: CoalescePolicy = CoalescePolicy.latest,
        job_executor: str | UnsetValue = unset,
        misfire_grace_time: float | timedelta | None | UnsetValue = unset,
        metadata: MetadataType | UnsetValue = unset,
        max_jitter: float | timedelta | None = None,
        job_result_expiration_time: float | timedelta = 0,
        conflict_policy: ConflictPolicy = ConflictPolicy.do_nothing,
    ) -> str:
        portal = self._ensure_services_ready()
        return portal.call(
            partial(
                self._async_scheduler.add_schedule,
                func_or_task_id,
                trigger,
                id=id,
                args=args,
                kwargs=kwargs,
                paused=paused,
                job_executor=job_executor,
                coalesce=coalesce,
                misfire_grace_time=misfire_grace_time,
                max_jitter=max_jitter,
                job_result_expiration_time=job_result_expiration_time,
                metadata=metadata,
                conflict_policy=conflict_policy,
            )
        )

    def get_schedule(self, id: str) -> Schedule:
        portal = self._ensure_services_ready()
        return portal.call(self._async_scheduler.get_schedule, id)

    def get_schedules(self) -> list[Schedule]:
        portal = self._ensure_services_ready()
        return portal.call(self._async_scheduler.get_schedules)

    def remove_schedule(self, id: str) -> None:
        portal = self._ensure_services_ready()
        portal.call(self._async_scheduler.remove_schedule, id)

    def pause_schedule(self, id: str) -> None:
        portal = self._ensure_services_ready()
        portal.call(self._async_scheduler.pause_schedule, id)

    def unpause_schedule(
        self,
        id: str,
        *,
        resume_from: datetime | Literal["now"] | None = None,
    ) -> None:
        portal = self._ensure_services_ready()
        portal.call(
            partial(
                self._async_scheduler.unpause_schedule,
                id,
                resume_from=resume_from,
            )
        )

    def add_job(
        self,
        func_or_task_id: TaskType,
        *,
        args: Iterable[Any] | None = None,
        kwargs: Mapping[str, Any] | None = None,
        job_executor: str | UnsetValue = unset,
        metadata: MetadataType | UnsetValue = unset,
        result_expiration_time: timedelta | float = 0,
    ) -> UUID:
        portal = self._ensure_services_ready()
        return portal.call(
            partial(
                self._async_scheduler.add_job,
                func_or_task_id,
                args=args,
                kwargs=kwargs,
                job_executor=job_executor,
                metadata=metadata,
                result_expiration_time=result_expiration_time,
            )
        )

    def get_jobs(self) -> Sequence[Job]:
        portal = self._ensure_services_ready()
        return portal.call(self._async_scheduler.get_jobs)

    def get_job_result(self, job_id: UUID, *, wait: bool = True) -> JobResult | None:
        portal = self._ensure_services_ready()
        return portal.call(
            partial(self._async_scheduler.get_job_result, job_id, wait=wait)
        )

    def run_job(
        self,
        func_or_task_id: str | Callable[..., Any],
        *,
        args: Iterable[Any] | None = None,
        kwargs: Mapping[str, Any] | None = None,
        job_executor: str | UnsetValue = unset,
        metadata: MetadataType | UnsetValue = unset,
    ) -> Any:
        portal = self._ensure_services_ready()
        return portal.call(
            partial(
                self._async_scheduler.run_job,
                func_or_task_id,
                args=args,
                kwargs=kwargs,
                job_executor=job_executor,
                metadata=metadata,
            )
        )

    def start_in_background(self) -> None:
        """
        Launch the scheduler in a new thread.

        This method registers :mod:`atexit` hooks to shut down the scheduler and wait
        for the thread to finish.

        :raises RuntimeError: if the scheduler is not in the ``stopped`` state

        """
        # Check if we're running under uWSGI with threads disabled
        uwsgi_module = sys.modules.get("uwsgi")
        if not getattr(uwsgi_module, "has_threads", True):
            raise RuntimeError(
                "The scheduler seems to be running under uWSGI, but threads have "
                "been disabled. You must run uWSGI with the --enable-threads "
                "option for the scheduler to work."
            )

        portal = self._ensure_services_ready()
        portal.call(self._async_scheduler.start_in_background)

    def stop(self) -> None:
        if self._portal is not None:
            self._portal.call(self._async_scheduler.stop)

    def wait_until_stopped(self) -> None:
        if self._portal is not None:
            self._portal.call(self._async_scheduler.wait_until_stopped)

    def run_until_stopped(self) -> None:
        with ExitStack() as exit_stack:
            # Run the async scheduler
            portal = self._ensure_services_ready(exit_stack)
            portal.call(self._async_scheduler.run_until_stopped)


# Copy the docstrings from the async variant
for attrname in dir(AsyncScheduler):
    if attrname.startswith("_"):
        continue

    value = getattr(AsyncScheduler, attrname)
    if callable(value):
        sync_method = getattr(Scheduler, attrname, None)
        if sync_method and not getattr(sync_method, "__doc__"):
            sync_method.__doc__ = value.__doc__
