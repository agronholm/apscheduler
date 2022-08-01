from __future__ import annotations

import sys
from datetime import datetime
from typing import Iterable
from uuid import UUID

import attrs
from anyio import to_thread
from anyio.from_thread import BlockingPortal

from .._enums import ConflictPolicy
from .._structures import Job, JobResult, Schedule, Task
from ..abc import AsyncEventBroker, DataStore
from ..eventbrokers.async_adapter import AsyncEventBrokerAdapter, SyncEventBrokerAdapter
from .base import BaseAsyncDataStore


@attrs.define(eq=False)
class AsyncDataStoreAdapter(BaseAsyncDataStore):
    original: DataStore
    _portal: BlockingPortal = attrs.field(init=False)

    async def start(self, event_broker: AsyncEventBroker) -> None:
        await super().start(event_broker)

        self._portal = BlockingPortal()
        await self._portal.__aenter__()

        if isinstance(event_broker, AsyncEventBrokerAdapter):
            sync_event_broker = event_broker.original
        else:
            sync_event_broker = SyncEventBrokerAdapter(event_broker, self._portal)

        try:
            await to_thread.run_sync(lambda: self.original.start(sync_event_broker))
        except BaseException:
            await self._portal.__aexit__(*sys.exc_info())
            raise

    async def stop(self, *, force: bool = False) -> None:
        try:
            await to_thread.run_sync(lambda: self.original.stop(force=force))
        finally:
            await self._portal.__aexit__(None, None, None)
            await super().stop(force=force)

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
