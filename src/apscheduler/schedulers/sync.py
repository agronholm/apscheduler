from __future__ import annotations

import atexit
import logging
import sys
import threading
from collections.abc import MutableMapping
from contextlib import ExitStack
from datetime import timedelta
from functools import partial
from logging import Logger
from types import TracebackType
from typing import Any, Callable, Iterable, Mapping
from uuid import UUID

from anyio import start_blocking_portal
from anyio.from_thread import BlockingPortal

from .. import Event, current_scheduler
from .._enums import CoalescePolicy, ConflictPolicy, RunState, SchedulerRole
from .._structures import JobResult, Schedule
from ..abc import DataStore, EventBroker, JobExecutor, Subscription, Trigger
from .async_ import AsyncScheduler

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class Scheduler:
    """A synchronous scheduler implementation."""

    def __init__(
        self,
        data_store: DataStore | None = None,
        event_broker: EventBroker | None = None,
        *,
        identity: str | None = None,
        role: SchedulerRole = SchedulerRole.both,
        job_executors: Mapping[str, JobExecutor] | None = None,
        default_job_executor: str | None = None,
        logger: Logger | None = None,
    ):
        kwargs: dict[str, Any] = {}
        if data_store is not None:
            kwargs["data_store"] = data_store
        if event_broker is not None:
            kwargs["event_broker"] = event_broker

        if not default_job_executor and not job_executors:
            default_job_executor = "threadpool"

        self._async_scheduler = AsyncScheduler(
            identity=identity,
            role=role,
            job_executors=job_executors,
            default_job_executor=default_job_executor,
            logger=logger or logging.getLogger(__name__),
            **kwargs,
        )
        self._exit_stack = ExitStack()
        self._portal: BlockingPortal | None = None
        self._lock = threading.RLock()

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
    def job_executors(self) -> MutableMapping[str, JobExecutor]:
        return self._async_scheduler.job_executors

    @property
    def default_job_executor(self) -> str:
        return self._async_scheduler.default_job_executor

    @default_job_executor.setter
    def default_job_executor(self, value: str) -> None:
        self._async_scheduler.default_job_executor = value

    @property
    def state(self) -> RunState:
        """The current running state of the scheduler."""
        return self._async_scheduler.state

    def __enter__(self: Self) -> Self:
        self._ensure_services_ready(self._exit_stack)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ) -> None:
        self._exit_stack.__exit__(exc_type, exc_val, exc_tb)

    def _ensure_services_ready(self, exit_stack: ExitStack | None = None) -> None:
        """Ensure that the underlying asynchronous scheduler has been initialized."""
        with self._lock:
            if self._portal is None:
                if exit_stack is None:
                    if self._exit_stack is None:
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

    def subscribe(
        self,
        callback: Callable[[Event], Any],
        event_types: Iterable[type[Event]] | None = None,
        *,
        one_shot: bool = False,
    ) -> Subscription:
        """
        Subscribe to events.

        To unsubscribe, call the :meth:`Subscription.unsubscribe` method on the returned
        object.

        :param callback: callable to be called with the event object when an event is
            published
        :param event_types: an iterable of concrete Event classes to subscribe to
        :param one_shot: if ``True``, automatically unsubscribe after the first matching
            event

        """
        return self.data_store.event_broker.subscribe(
            callback, event_types, is_async=False, one_shot=one_shot
        )

    def add_schedule(
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
        tags: Iterable[str] | None = None,
        conflict_policy: ConflictPolicy = ConflictPolicy.do_nothing,
    ) -> str:
        self._ensure_services_ready()
        return self._portal.call(
            partial(
                self._async_scheduler.add_schedule,
                func_or_task_id,
                trigger,
                id=id,
                args=args,
                kwargs=kwargs,
                job_executor=job_executor,
                coalesce=coalesce,
                misfire_grace_time=misfire_grace_time,
                max_jitter=max_jitter,
                tags=tags,
                conflict_policy=conflict_policy,
            )
        )

    def get_schedule(self, id: str) -> Schedule:
        self._ensure_services_ready()
        return self._portal.call(self._async_scheduler.get_schedule, id)

    def get_schedules(self) -> list[Schedule]:
        self._ensure_services_ready()
        return self._portal.call(self._async_scheduler.get_schedules)

    def remove_schedule(self, id: str) -> None:
        self._ensure_services_ready()
        self._portal.call(self._async_scheduler.remove_schedule, id)

    def add_job(
        self,
        func_or_task_id: str | Callable,
        *,
        args: Iterable | None = None,
        kwargs: Mapping[str, Any] | None = None,
        job_executor: str | None = None,
        tags: Iterable[str] | None = None,
        result_expiration_time: timedelta | float = 0,
    ) -> UUID:
        self._ensure_services_ready()
        return self._portal.call(
            partial(
                self._async_scheduler.add_job,
                func_or_task_id,
                args=args,
                kwargs=kwargs,
                job_executor=job_executor,
                tags=tags,
                result_expiration_time=result_expiration_time,
            )
        )

    def get_job_result(self, job_id: UUID, *, wait: bool = True) -> JobResult:
        self._ensure_services_ready()
        return self._portal.call(
            partial(self._async_scheduler.get_job_result, job_id, wait=wait)
        )

    def run_job(
        self,
        func_or_task_id: str | Callable,
        *,
        args: Iterable | None = None,
        kwargs: Mapping[str, Any] | None = None,
        job_executor: str | None = None,
        tags: Iterable[str] | None = (),
    ) -> Any:
        self._ensure_services_ready()
        return self._portal.call(
            partial(
                self._async_scheduler.run_job,
                func_or_task_id,
                args=args,
                kwargs=kwargs,
                job_executor=job_executor,
                tags=tags,
            )
        )

    def start_in_background(self) -> None:
        """
        Launch the scheduler in a new thread.

        This method registers :mod:`atexit` hooks to shut down the scheduler and wait
        for the thread to finish.

        :raises RuntimeError: if the scheduler is not in the ``stopped`` state

        """
        self._ensure_services_ready()
        self._portal.call(self._async_scheduler.start_in_background)

    def stop(self) -> None:
        if self._portal is not None:
            self._portal.call(self._async_scheduler.stop)

    def wait_until_stopped(self) -> None:
        if self._portal is not None:
            self._portal.call(self._async_scheduler.wait_until_stopped)

    def run_until_stopped(self) -> None:
        with ExitStack() as exit_stack:
            # Run the async scheduler
            self._ensure_services_ready(exit_stack)
            self._portal.call(self._async_scheduler.run_until_stopped)
