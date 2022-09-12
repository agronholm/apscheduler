from __future__ import annotations

from collections.abc import Mapping
from contextlib import AsyncExitStack
from datetime import datetime, timezone
from logging import Logger, getLogger
from typing import Callable
from uuid import UUID

import anyio
import attrs
from anyio import create_task_group, get_cancelled_exc_class, move_on_after
from anyio.abc import CancelScope

from ._context import current_job
from ._enums import JobOutcome, RunState
from ._events import JobAdded, JobReleased, WorkerStarted, WorkerStopped
from ._structures import Job, JobInfo, JobResult
from ._validators import positive_integer
from .abc import DataStore, EventBroker, JobExecutor


@attrs.define(eq=False, kw_only=True)
class Worker:
    """
    Runs jobs locally in a task group.

    :param max_concurrent_jobs: Maximum number of jobs the worker will run at once
    """

    job_executors: Mapping[str, JobExecutor] = attrs.field(kw_only=True)
    max_concurrent_jobs: int = attrs.field(
        kw_only=True, validator=positive_integer, default=100
    )
    logger: Logger = attrs.field(kw_only=True, default=getLogger(__name__))

    _data_store: DataStore = attrs.field(init=False)
    _event_broker: EventBroker = attrs.field(init=False)
    _identity: str = attrs.field(init=False)
    _state: RunState = attrs.field(init=False, default=RunState.stopped)
    _wakeup_event: anyio.Event = attrs.field(init=False)
    _acquired_jobs: set[Job] = attrs.field(init=False, factory=set)
    _running_jobs: set[UUID] = attrs.field(init=False, factory=set)

    async def start(
        self,
        exit_stack: AsyncExitStack,
        data_store: DataStore,
        event_broker: EventBroker,
        identity: str,
    ) -> None:
        self._data_store = data_store
        self._event_broker = event_broker
        self._identity = identity
        self._state = RunState.started
        self._wakeup_event = anyio.Event()

        # Start the job executors
        for job_executor in self.job_executors.values():
            await job_executor.start(exit_stack)

        # Start the worker in a background task
        task_group = await exit_stack.enter_async_context(create_task_group())
        task_group.start_soon(self._run)

        # Stop the worker when the exit stack unwinds
        exit_stack.callback(lambda: self._wakeup_event.set())
        exit_stack.callback(setattr, self, "_state", RunState.stopped)

        # Wake up the worker if the data store emits a significant job event
        exit_stack.enter_context(
            self._event_broker.subscribe(
                lambda event: self._wakeup_event.set(), {JobAdded}
            )
        )

        # Signal that the worker has started
        await self._event_broker.publish_local(WorkerStarted())

    async def _run(self) -> None:
        """Run the worker until it is explicitly stopped."""
        exception: BaseException | None = None
        try:
            async with create_task_group() as tg:
                while self._state is RunState.started:
                    limit = self.max_concurrent_jobs - len(self._running_jobs)
                    jobs = await self._data_store.acquire_jobs(self._identity, limit)
                    for job in jobs:
                        task = await self._data_store.get_task(job.task_id)
                        self._running_jobs.add(job.id)
                        tg.start_soon(self._run_job, job, task.func, task.executor)

                    await self._wakeup_event.wait()
                    self._wakeup_event = anyio.Event()
        except get_cancelled_exc_class():
            pass
        except BaseException as exc:
            exception = exc
            raise
        finally:
            if not exception:
                self.logger.info("Worker stopped")
            elif isinstance(exception, Exception):
                self.logger.exception("Worker crashed")
            elif exception:
                self.logger.info(
                    f"Worker stopped due to {exception.__class__.__name__}"
                )

            with move_on_after(3, shield=True):
                await self._event_broker.publish_local(
                    WorkerStopped(exception=exception)
                )

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
                await self._data_store.release_job(self._identity, job.task_id, result)
                await self._event_broker.publish(
                    JobReleased.from_result(result, self._identity)
                )
                return

            try:
                job_executor = self.job_executors[executor]
            except KeyError:
                return

            token = current_job.set(JobInfo.from_job(job))
            try:
                retval = await job_executor.run_job(func, job)
            except get_cancelled_exc_class():
                self.logger.info("Job %s was cancelled", job.id)
                with CancelScope(shield=True):
                    result = JobResult.from_job(
                        job,
                        outcome=JobOutcome.cancelled,
                    )
                    await self._data_store.release_job(
                        self._identity, job.task_id, result
                    )
                    await self._event_broker.publish(
                        JobReleased.from_result(result, self._identity)
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
                await self._data_store.release_job(
                    self._identity,
                    job.task_id,
                    result,
                )
                await self._event_broker.publish(
                    JobReleased.from_result(result, self._identity)
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
                await self._data_store.release_job(self._identity, job.task_id, result)
                await self._event_broker.publish(
                    JobReleased.from_result(result, self._identity)
                )
            finally:
                current_job.reset(token)
        finally:
            self._running_jobs.remove(job.id)
