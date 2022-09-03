from __future__ import annotations

import atexit
import os
import platform
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import ExitStack
from contextvars import copy_context
from datetime import datetime, timezone
from logging import Logger, getLogger
from types import TracebackType
from typing import Callable
from uuid import UUID

import attrs

from .. import JobReleased
from .._context import current_job, current_worker
from .._enums import JobOutcome, RunState
from .._events import JobAdded, WorkerStarted, WorkerStopped
from .._structures import Job, JobInfo, JobResult
from .._validators import positive_integer
from ..abc import DataStore, EventBroker
from ..eventbrokers.local import LocalEventBroker


@attrs.define(eq=False)
class Worker:
    """Runs jobs locally in a thread pool."""

    data_store: DataStore
    event_broker: EventBroker = attrs.field(factory=LocalEventBroker)
    max_concurrent_jobs: int = attrs.field(
        kw_only=True, validator=positive_integer, default=20
    )
    identity: str = attrs.field(kw_only=True, default=None)
    logger: Logger | None = attrs.field(kw_only=True, default=getLogger(__name__))
    # True if a scheduler owns this worker
    _is_internal: bool = attrs.field(kw_only=True, default=False)

    _state: RunState = attrs.field(init=False, default=RunState.stopped)
    _thread: threading.Thread | None = attrs.field(init=False, default=None)
    _wakeup_event: threading.Event = attrs.field(init=False, factory=threading.Event)
    _executor: ThreadPoolExecutor = attrs.field(init=False)
    _acquired_jobs: set[Job] = attrs.field(init=False, factory=set)
    _running_jobs: set[UUID] = attrs.field(init=False, factory=set)

    def __attrs_post_init__(self) -> None:
        if not self.identity:
            self.identity = f"{platform.node()}-{os.getpid()}-{id(self)}"

    def __enter__(self) -> Worker:
        self.start_in_background()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ) -> None:
        self.stop()

    @property
    def state(self) -> RunState:
        """The current running state of the worker."""
        return self._state

    def start_in_background(self) -> None:
        """
        Launch the worker in a new thread.

        This method registers an :mod:`atexit` hook to shut down the worker and wait
        for the thread to finish.

        """
        start_future: Future[None] = Future()
        self._thread = threading.Thread(
            target=copy_context().run, args=[self._run, start_future], daemon=True
        )
        self._thread.start()
        try:
            start_future.result()
        except BaseException:
            self._thread = None
            raise

        atexit.register(self.stop)

    def stop(self) -> None:
        """
        Signal the worker that it should stop running jobs.

        This method does not wait for the worker to actually stop.

        """
        atexit.unregister(self.stop)
        if self._state is RunState.started:
            self._state = RunState.stopping
            self._wakeup_event.set()

            if threading.current_thread() != self._thread:
                self._thread.join()
                self._thread = None

    def run_until_stopped(self) -> None:
        """
        Run the worker until it is explicitly stopped.

        This method will only return if :meth:`stop` is called.

        """
        self._run(None)

    def _run(self, start_future: Future[None] | None) -> None:
        with ExitStack() as exit_stack:
            try:
                if self._state is not RunState.stopped:
                    raise RuntimeError(
                        f'Cannot start the worker when it is in the "{self._state}" '
                        f"state"
                    )

                if not self._is_internal:
                    # Initialize the event broker
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

                # Set the current worker
                token = current_worker.set(self)
                exit_stack.callback(current_worker.reset, token)

                # Wake up the worker if the data store emits a significant job event
                exit_stack.enter_context(
                    self.event_broker.subscribe(
                        lambda event: self._wakeup_event.set(), {JobAdded}
                    )
                )

                # Initialize the thread pool
                executor = ThreadPoolExecutor(max_workers=self.max_concurrent_jobs)
                exit_stack.enter_context(executor)

                # Signal that the worker has started
                self._state = RunState.started
                self.event_broker.publish_local(WorkerStarted())
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
                    available_slots = self.max_concurrent_jobs - len(self._running_jobs)
                    if available_slots:
                        jobs = self.data_store.acquire_jobs(
                            self.identity, available_slots
                        )
                        for job in jobs:
                            task = self.data_store.get_task(job.task_id)
                            self._running_jobs.add(job.id)
                            executor.submit(
                                copy_context().run, self._run_job, job, task.func
                            )

                    self._wakeup_event.wait()
                    self._wakeup_event = threading.Event()
            except BaseException as exc:
                exception = exc
                raise
            finally:
                self._state = RunState.stopped
                if not exception:
                    self.logger.info("Worker stopped")
                elif isinstance(exception, Exception):
                    self.logger.exception("Worker crashed")
                elif exception:
                    self.logger.info(
                        f"Worker stopped due to {exception.__class__.__name__}"
                    )

                self.event_broker.publish_local(WorkerStopped(exception=exception))

    def _run_job(self, job: Job, func: Callable) -> None:
        try:
            # Check if the job started before the deadline
            start_time = datetime.now(timezone.utc)
            if job.start_deadline is not None and start_time > job.start_deadline:
                result = JobResult.from_job(
                    job, JobOutcome.missed_start_deadline, finished_at=start_time
                )
                self.event_broker.publish(
                    JobReleased.from_result(result, self.identity)
                )
                self.data_store.release_job(self.identity, job.task_id, result)
                return

            token = current_job.set(JobInfo.from_job(job))
            try:
                retval = func(*job.args, **job.kwargs)
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
                self.data_store.release_job(
                    self.identity,
                    job.task_id,
                    result,
                )
                self.event_broker.publish(
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
                self.data_store.release_job(self.identity, job.task_id, result)
                self.event_broker.publish(
                    JobReleased.from_result(result, self.identity)
                )
            finally:
                current_job.reset(token)
        finally:
            self._running_jobs.remove(job.id)
