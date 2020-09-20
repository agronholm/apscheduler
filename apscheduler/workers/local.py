from __future__ import annotations

from collections import OrderedDict
from collections.abc import Coroutine
from dataclasses import dataclass, field
from datetime import datetime, timezone
from traceback import format_exc
from typing import Any, Callable, List

from anyio import create_task_group
from anyio.abc import TaskGroup
from apscheduler.abc import EventHub, Executor, Job
from apscheduler.eventhubs.local import LocalEventHub
from apscheduler.events import (
    Event, JobAdded, JobDeadlineMissed, JobFailed, JobSuccessful, JobUpdated)


@dataclass
class LocalExecutor(Executor):
    """Runs jobs locally in a task group."""

    _event_hub: EventHub = field(init=False, default_factory=LocalEventHub)
    _task_group: TaskGroup = field(init=False)
    _queued_jobs: OrderedDict[Job, None] = field(init=False, default_factory=OrderedDict)
    _running_jobs: OrderedDict[Job, None] = field(init=False, default_factory=OrderedDict)

    async def __aenter__(self) -> LocalExecutor:
        self._task_group = create_task_group()
        await self._task_group.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self._task_group.__aexit__(exc_type, exc_val, exc_tb)

    async def _run_job(self) -> None:
        job = self._queued_jobs.popitem(last=False)[0]

        # Check if the job started before the deadline
        if job.start_deadline:
            tz = job.scheduled_start_time.tzinfo
            start_time = datetime.now(tz)
            if start_time >= job.start_deadline:
                event = JobDeadlineMissed(
                    start_time, job_id=job.id, task_id=job.task_id, schedule_id=job.schedule_id,
                    scheduled_start_time=job.scheduled_start_time,
                    start_deadline=job.start_deadline
                )
                await self._event_hub.publish(event)
                return
        else:
            tz = timezone.utc
            start_time = datetime.now(tz)

        # Set the job as running and publish a job update event
        self._running_jobs[job] = None
        job.started_at = start_time
        event = JobUpdated(
            timestamp=datetime.now(tz), job_id=job.id, task_id=job.task_id,
            schedule_id=job.schedule_id, scheduled_start_time=job.scheduled_start_time
        )
        await self._event_hub.publish(event)

        try:
            return_value = job.func(*job.args, **job.kwargs)
            if isinstance(return_value, Coroutine):
                return_value = await return_value
        except Exception as exc:
            event = JobFailed(
                timestamp=datetime.now(tz), job_id=job.id, task_id=job.task_id,
                schedule_id=job.schedule_id, scheduled_start_time=job.scheduled_start_time,
                start_time=start_time, start_deadline=job.start_deadline,
                formatted_traceback=format_exc(), exception=exc)
        else:
            event = JobSuccessful(
                timestamp=datetime.now(tz), job_id=job.id, task_id=job.task_id,
                schedule_id=job.schedule_id, scheduled_start_time=job.scheduled_start_time,
                start_time=start_time, start_deadline=job.start_deadline, return_value=return_value
            )

        del self._running_jobs[job]
        await self._event_hub.publish(event)

    async def submit_job(self, job: Job) -> None:
        self._queued_jobs[job] = None
        await self._task_group.spawn(self._run_job)

        event = JobAdded(datetime.now(timezone.utc), job.id, job.task_id, job.schedule_id,
                         job.scheduled_start_time)
        await self._event_hub.publish(event)

    async def get_jobs(self) -> List[Job]:
        return list(self._queued_jobs)

    async def subscribe(self, callback: Callable[[Event], Any]) -> None:
        await self._event_hub.subscribe(callback)
