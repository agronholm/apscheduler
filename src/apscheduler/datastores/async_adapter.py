from __future__ import annotations

from contextlib import AsyncExitStack
from datetime import datetime
from functools import partial
from typing import Iterable
from uuid import UUID

import attrs
from anyio import to_thread
from anyio.from_thread import BlockingPortal

from ..abc import AsyncDataStore, AsyncEventBroker, DataStore, EventSource
from ..enums import ConflictPolicy
from ..eventbrokers.async_adapter import AsyncEventBrokerAdapter
from ..structures import Job, JobResult, Schedule, Task
from ..util import reentrant


@reentrant
@attrs.define(eq=False)
class AsyncDataStoreAdapter(AsyncDataStore):
    original: DataStore
    _portal: BlockingPortal = attrs.field(init=False)
    _events: AsyncEventBroker = attrs.field(init=False)
    _exit_stack: AsyncExitStack = attrs.field(init=False)

    @property
    def events(self) -> EventSource:
        return self._events

    async def __aenter__(self) -> AsyncDataStoreAdapter:
        self._exit_stack = AsyncExitStack()

        self._portal = BlockingPortal()
        await self._exit_stack.enter_async_context(self._portal)

        self._events = AsyncEventBrokerAdapter(self.original.events, self._portal)
        await self._exit_stack.enter_async_context(self._events)

        await to_thread.run_sync(self.original.__enter__)
        self._exit_stack.push_async_exit(
            partial(to_thread.run_sync, self.original.__exit__)
        )

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._exit_stack.__aexit__(exc_type, exc_val, exc_tb)

    async def add_task(self, task: Task) -> None:
        await to_thread.run_sync(self.original.add_task, task)

    async def remove_task(self, task_id: str) -> None:
        await to_thread.run_sync(self.original.remove_task, task_id)

    async def get_task(self, task_id: str) -> Task:
        return await to_thread.run_sync(self.original.get_task, task_id)

    async def get_tasks(self) -> list[Task]:
        return await to_thread.run_sync(self.original.get_tasks)

    async def get_schedules(self, ids: set[str] | None = None) -> list[Schedule]:
        return await to_thread.run_sync(self.original.get_schedules, ids)

    async def add_schedule(
        self, schedule: Schedule, conflict_policy: ConflictPolicy
    ) -> None:
        await to_thread.run_sync(self.original.add_schedule, schedule, conflict_policy)

    async def remove_schedules(self, ids: Iterable[str]) -> None:
        await to_thread.run_sync(self.original.remove_schedules, ids)

    async def acquire_schedules(self, scheduler_id: str, limit: int) -> list[Schedule]:
        return await to_thread.run_sync(
            self.original.acquire_schedules, scheduler_id, limit
        )

    async def release_schedules(
        self, scheduler_id: str, schedules: list[Schedule]
    ) -> None:
        await to_thread.run_sync(
            self.original.release_schedules, scheduler_id, schedules
        )

    async def get_next_schedule_run_time(self) -> datetime | None:
        return await to_thread.run_sync(self.original.get_next_schedule_run_time)

    async def add_job(self, job: Job) -> None:
        await to_thread.run_sync(self.original.add_job, job)

    async def get_jobs(self, ids: Iterable[UUID] | None = None) -> list[Job]:
        return await to_thread.run_sync(self.original.get_jobs, ids)

    async def acquire_jobs(self, worker_id: str, limit: int | None = None) -> list[Job]:
        return await to_thread.run_sync(self.original.acquire_jobs, worker_id, limit)

    async def release_job(
        self, worker_id: str, task_id: str, result: JobResult
    ) -> None:
        await to_thread.run_sync(self.original.release_job, worker_id, task_id, result)

    async def get_job_result(self, job_id: UUID) -> JobResult | None:
        return await to_thread.run_sync(self.original.get_job_result, job_id)
