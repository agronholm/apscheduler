from __future__ import annotations

import os
import platform
import threading
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from contextlib import ExitStack
from contextvars import copy_context
from datetime import datetime, timezone
from logging import Logger, getLogger
from typing import Callable, Optional
from uuid import UUID

import attr

from ..abc import DataStore, EventSource
from ..context import current_worker, job_info
from ..enums import JobOutcome, RunState
from ..eventbrokers.local import LocalEventBroker
from ..events import JobAdded, WorkerStarted, WorkerStopped
from ..structures import Job, JobInfo, JobResult
from ..validators import positive_integer


@attr.define(eq=False)
class Worker:
    """Runs jobs locally in a thread pool."""
    data_store: DataStore
    max_concurrent_jobs: int = attr.field(kw_only=True, validator=positive_integer, default=20)
    identity: str = attr.field(kw_only=True, default=None)
    logger: Optional[Logger] = attr.field(kw_only=True, default=getLogger(__name__))

    _state: RunState = attr.field(init=False, default=RunState.stopped)
    _wakeup_event: threading.Event = attr.field(init=False)
    _acquired_jobs: set[Job] = attr.field(init=False, factory=set)
    _events: LocalEventBroker = attr.field(init=False, factory=LocalEventBroker)
    _running_jobs: set[UUID] = attr.field(init=False, factory=set)
    _exit_stack: ExitStack = attr.field(init=False)
    _executor: ThreadPoolExecutor = attr.field(init=False)

    def __attrs_post_init__(self) -> None:
        if not self.identity:
            self.identity = f'{platform.node()}-{os.getpid()}-{id(self)}'

    @property
    def events(self) -> EventSource:
        return self._events

    @property
    def state(self) -> RunState:
        return self._state

    def __enter__(self) -> Worker:
        self._state = RunState.starting
        self._wakeup_event = threading.Event()
        self._exit_stack = ExitStack()
        self._exit_stack.__enter__()
        self._exit_stack.enter_context(self._events)

        # Initialize the data store and start relaying events to the worker's event broker
        self._exit_stack.enter_context(self.data_store)
        self._exit_stack.enter_context(self.data_store.events.subscribe(self._events.publish))

        # Wake up the worker if the data store emits a significant job event
        self._exit_stack.enter_context(
            self.data_store.events.subscribe(lambda event: self._wakeup_event.set(), {JobAdded})
        )

        # Start the worker and return when it has signalled readiness or raised an exception
        start_future: Future[None] = Future()
        with self._events.subscribe(start_future.set_result, one_shot=True):
            self._executor = ThreadPoolExecutor(1)
            run_future = self._executor.submit(copy_context().run, self.run)
            wait([start_future, run_future], return_when=FIRST_COMPLETED)

        if run_future.done():
            run_future.result()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._state = RunState.stopping
        self._wakeup_event.set()
        self._executor.shutdown(wait=exc_type is None)
        self._exit_stack.__exit__(exc_type, exc_val, exc_tb)
        del self._wakeup_event

    def run(self) -> None:
        if self._state is not RunState.starting:
            raise RuntimeError(f'This function cannot be called while the worker is in the '
                               f'{self._state} state')

        # Set the current worker
        token = current_worker.set(self)

        # Signal that the worker has started
        self._state = RunState.started
        self._events.publish(WorkerStarted())

        executor = ThreadPoolExecutor(max_workers=self.max_concurrent_jobs)
        try:
            while self._state is RunState.started:
                available_slots = self.max_concurrent_jobs - len(self._running_jobs)
                if available_slots:
                    jobs = self.data_store.acquire_jobs(self.identity, available_slots)
                    for job in jobs:
                        task = self.data_store.get_task(job.task_id)
                        self._running_jobs.add(job.id)
                        executor.submit(copy_context().run, self._run_job, job, task.func)

                self._wakeup_event.wait()
                self._wakeup_event = threading.Event()
        except BaseException as exc:
            executor.shutdown(wait=False)
            self._state = RunState.stopped
            self._events.publish(WorkerStopped(exception=exc))
            raise

        executor.shutdown()
        current_worker.reset(token)
        self._state = RunState.stopped
        self._events.publish(WorkerStopped())

    def _run_job(self, job: Job, func: Callable) -> None:
        try:
            # Check if the job started before the deadline
            start_time = datetime.now(timezone.utc)
            if job.start_deadline is not None and start_time > job.start_deadline:
                result = JobResult(job_id=job.id, outcome=JobOutcome.missed_start_deadline)
                self.data_store.release_job(self.identity, job.task_id, result)
                return

            token = job_info.set(JobInfo.from_job(job))
            try:
                retval = func(*job.args, **job.kwargs)
            except BaseException as exc:
                result = JobResult(job_id=job.id, outcome=JobOutcome.error, exception=exc)
                self.data_store.release_job(self.identity, job.task_id, result)
                if not isinstance(exc, Exception):
                    raise
            else:
                result = JobResult(job_id=job.id, outcome=JobOutcome.success, return_value=retval)
                self.data_store.release_job(self.identity, job.task_id, result)
            finally:
                job_info.reset(token)
        finally:
            self._running_jobs.remove(job.id)
