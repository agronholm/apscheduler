from __future__ import annotations

from datetime import datetime
from functools import partial
from typing import Any, Callable, Iterable, Optional, Type
from uuid import UUID

import attr
from anyio import to_thread
from anyio.from_thread import BlockingPortal

from ... import events
from ...abc import AsyncDataStore, DataStore
from ...enums import ConflictPolicy
from ...events import Event, SubscriptionToken
from ...structures import Job, JobResult, Schedule, Task
from ...util import reentrant


@reentrant
@attr.define(eq=False)
class AsyncDataStoreAdapter(AsyncDataStore):
    original: DataStore
    _portal: BlockingPortal = attr.field(init=False, eq=False)

    async def __aenter__(self) -> AsyncDataStoreAdapter:
        self._portal = BlockingPortal()
        await self._portal.__aenter__()
        await to_thread.run_sync(self.original.__enter__)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await to_thread.run_sync(self.original.__exit__, exc_type, exc_val, exc_tb)
        await self._portal.__aexit__(exc_type, exc_val, exc_tb)

    async def add_task(self, task: Task) -> None:
        await to_thread.run_sync(self.original.add_task, task)

    async def remove_task(self, task_id: str) -> None:
        await to_thread.run_sync(self.original.remove_task, task_id)

    async def get_task(self, task_id: str) -> Task:
        return await to_thread.run_sync(self.original.get_task, task_id)

    async def get_tasks(self) -> list[Task]:
        return await to_thread.run_sync(self.original.get_tasks)

    async def get_schedules(self, ids: Optional[set[str]] = None) -> list[Schedule]:
        return await to_thread.run_sync(self.original.get_schedules, ids)

    async def add_schedule(self, schedule: Schedule, conflict_policy: ConflictPolicy) -> None:
        await to_thread.run_sync(self.original.add_schedule, schedule, conflict_policy)

    async def remove_schedules(self, ids: Iterable[str]) -> None:
        await to_thread.run_sync(self.original.remove_schedules, ids)

    async def acquire_schedules(self, scheduler_id: str, limit: int) -> list[Schedule]:
        return await to_thread.run_sync(self.original.acquire_schedules, scheduler_id, limit)

    async def release_schedules(self, scheduler_id: str, schedules: list[Schedule]) -> None:
        await to_thread.run_sync(self.original.release_schedules, scheduler_id, schedules)

    async def get_next_schedule_run_time(self) -> Optional[datetime]:
        return await to_thread.run_sync(self.original.get_next_schedule_run_time)

    async def add_job(self, job: Job) -> None:
        await to_thread.run_sync(self.original.add_job, job)

    async def get_jobs(self, ids: Optional[Iterable[UUID]] = None) -> list[Job]:
        return await to_thread.run_sync(self.original.get_jobs, ids)

    async def acquire_jobs(self, worker_id: str, limit: Optional[int] = None) -> list[Job]:
        return await to_thread.run_sync(self.original.acquire_jobs, worker_id, limit)

    async def release_job(self, worker_id: str, job: Job, result: Optional[JobResult]) -> None:
        await to_thread.run_sync(self.original.release_job, worker_id, job, result)

    async def get_job_result(self, job_id: UUID) -> Optional[JobResult]:
        return await to_thread.run_sync(self.original.get_job_result, job_id)

    def subscribe(self, callback: Callable[[Event], Any],
                  event_types: Optional[Iterable[Type[Event]]] = None) -> SubscriptionToken:
        return self.original.subscribe(partial(self._portal.call, callback), event_types)

    def unsubscribe(self, token: events.SubscriptionToken) -> None:
        self.original.unsubscribe(token)
