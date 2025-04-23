from __future__ import annotations

from bisect import bisect_left, bisect_right, insort_right
from collections import defaultdict
from collections.abc import Iterable, Sequence
from datetime import MAXYEAR, datetime, timedelta, timezone
from functools import partial
from uuid import UUID

import attrs

from .. import JobOutcome
from .._enums import ConflictPolicy
from .._events import (
    JobAcquired,
    JobAdded,
    JobReleased,
    ScheduleAdded,
    ScheduleRemoved,
    ScheduleUpdated,
    TaskAdded,
    TaskRemoved,
    TaskUpdated,
)
from .._exceptions import ConflictingIdError, TaskLookupError
from .._structures import Job, JobResult, Schedule, ScheduleResult, Task
from .._utils import create_repr
from .base import BaseDataStore

max_datetime = datetime(MAXYEAR, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc)


@attrs.define(eq=False, repr=False)
class MemoryDataStore(BaseDataStore):
    """
    Stores scheduler data in memory, without serializing it.

    Can be shared between multiple schedulers within the same event loop.
    """

    _tasks: dict[str, Task] = attrs.Factory(dict)
    _schedules: list[Schedule] = attrs.Factory(list)
    _schedules_by_id: dict[str, Schedule] = attrs.Factory(dict)
    _schedules_by_task_id: dict[str, set[Schedule]] = attrs.Factory(
        partial(defaultdict, set)
    )
    _jobs_by_id: dict[UUID, Job] = attrs.Factory(dict)
    _jobs_by_task_id: dict[str, set[Job]] = attrs.Factory(partial(defaultdict, set))
    _jobs_by_schedule_id: dict[str, set[Job]] = attrs.Factory(partial(defaultdict, set))
    _job_results: dict[UUID, JobResult] = attrs.Factory(dict)

    def __repr__(self) -> str:
        return create_repr(self)

    def _find_schedule_index(self, schedule: Schedule) -> int:
        left_index = bisect_left(self._schedules, schedule)
        right_index = bisect_right(self._schedules, schedule)
        return self._schedules.index(schedule, left_index, right_index + 1)

    async def get_schedules(self, ids: set[str] | None = None) -> list[Schedule]:
        if ids is None:
            return self._schedules.copy()

        return [
            schedule
            for schedule in self._schedules
            if ids is None or schedule.id in ids
        ]

    async def add_task(self, task: Task) -> None:
        if task.id in self._tasks:
            task.running_jobs = self._tasks[task.id].running_jobs
            self._tasks[task.id] = task
            await self._event_broker.publish(TaskUpdated(task_id=task.id))
        else:
            self._tasks[task.id] = task
            await self._event_broker.publish(TaskAdded(task_id=task.id))

    async def remove_task(self, task_id: str) -> None:
        try:
            del self._tasks[task_id]
        except KeyError:
            raise TaskLookupError(task_id) from None

        await self._event_broker.publish(TaskRemoved(task_id=task_id))

    async def get_task(self, task_id: str) -> Task:
        try:
            return self._tasks[task_id]
        except KeyError:
            raise TaskLookupError(task_id) from None

    async def get_tasks(self) -> list[Task]:
        return sorted(self._tasks.values())

    async def add_schedule(
        self, schedule: Schedule, conflict_policy: ConflictPolicy
    ) -> None:
        old_schedule = self._schedules_by_id.get(schedule.id)
        if old_schedule is not None:
            if conflict_policy is ConflictPolicy.do_nothing:
                return
            elif conflict_policy is ConflictPolicy.exception:
                raise ConflictingIdError(schedule.id)

            index = self._find_schedule_index(old_schedule)
            del self._schedules[index]
            self._schedules_by_task_id[old_schedule.task_id].remove(old_schedule)

        self._schedules_by_id[schedule.id] = schedule
        self._schedules_by_task_id[schedule.task_id].add(schedule)
        insort_right(self._schedules, schedule)

        event: ScheduleUpdated | ScheduleAdded
        if old_schedule is not None:
            event = ScheduleUpdated(
                schedule_id=schedule.id,
                task_id=schedule.task_id,
                next_fire_time=schedule.next_fire_time,
            )
        else:
            event = ScheduleAdded(
                schedule_id=schedule.id,
                task_id=schedule.task_id,
                next_fire_time=schedule.next_fire_time,
            )

        await self._event_broker.publish(event)

    async def remove_schedules(
        self, ids: Iterable[str], *, finished: bool = False
    ) -> None:
        for schedule_id in ids:
            schedule = self._schedules_by_id.pop(schedule_id, None)
            if schedule:
                self._schedules.remove(schedule)
                event = ScheduleRemoved(
                    schedule_id=schedule.id,
                    task_id=schedule.task_id,
                    finished=finished,
                )
                await self._event_broker.publish(event)

    async def acquire_schedules(
        self, scheduler_id: str, lease_duration: timedelta, limit: int
    ) -> list[Schedule]:
        now = datetime.now(timezone.utc)
        acquired_until = now + lease_duration
        schedules: list[Schedule] = []
        for schedule in self._schedules:
            if schedule.next_fire_time is None or schedule.next_fire_time > now:
                # The schedule is either exhausted or not yet due. There will be no
                # schedules that are due after this one, so we can stop here.
                break
            elif schedule.paused:
                # The schedule is paused
                continue
            elif schedule.acquired_until is not None:
                if (
                    schedule.acquired_by != scheduler_id
                    and now <= schedule.acquired_until
                ):
                    # The schedule has been acquired by another scheduler and the
                    # timeout has not expired yet
                    continue

            schedules.append(schedule)
            schedule.acquired_by = scheduler_id
            schedule.acquired_until = acquired_until
            if len(schedules) == limit:
                break

        return schedules

    async def release_schedules(
        self, scheduler_id: str, results: Sequence[ScheduleResult]
    ) -> None:
        # Send update events for schedules
        for result in results:
            # Remove the schedule
            schedule = self._schedules_by_id[result.schedule_id]
            index = self._find_schedule_index(schedule)
            del self._schedules[index]

            # Re-add the schedule to its new position
            schedule.last_fire_time = result.last_fire_time
            schedule.next_fire_time = result.next_fire_time
            schedule.acquired_by = None
            schedule.acquired_until = None
            insort_right(self._schedules, schedule)
            event = ScheduleUpdated(
                schedule_id=result.schedule_id,
                task_id=schedule.task_id,
                next_fire_time=result.next_fire_time,
            )
            await self._event_broker.publish(event)

    async def get_next_schedule_run_time(self) -> datetime | None:
        return self._schedules[0].next_fire_time if self._schedules else None

    async def add_job(self, job: Job) -> None:
        self._jobs_by_id[job.id] = job
        self._jobs_by_task_id[job.task_id].add(job)
        if job.schedule_id is not None:
            self._jobs_by_schedule_id[job.schedule_id].add(job)

        event = JobAdded(
            job_id=job.id,
            task_id=job.task_id,
            schedule_id=job.schedule_id,
        )
        await self._event_broker.publish(event)

    async def get_jobs(self, ids: Iterable[UUID] | None = None) -> list[Job]:
        if ids is not None:
            ids = frozenset(ids)

        if ids is None:
            return list(self._jobs_by_id.values())

        return [
            job for job in self._jobs_by_id.values() if ids is None or job.id in ids
        ]

    async def acquire_jobs(
        self, scheduler_id: str, lease_duration: timedelta, limit: int | None = None
    ) -> list[Job]:
        now = datetime.now(timezone.utc)
        acquired_until = now + lease_duration
        jobs: list[Job] = []
        job_results: dict[Job, JobResult] = {}
        for job in self._jobs_by_id.values():
            task = self._tasks[job.task_id]

            # Skip already acquired jobs (unless the acquisition lock has expired)
            if job.acquired_until is not None:
                if job.acquired_until >= now:
                    continue
                else:
                    task.running_jobs -= 1

            # Discard the job if its start deadline has passed
            if job.start_deadline and job.start_deadline < now:
                job_results[job] = JobResult(
                    job_id=job.id,
                    outcome=JobOutcome.missed_start_deadline,
                    finished_at=now,
                    expires_at=now + job.result_expiration_time,
                )
                continue

            # Skip the job if no more slots are available
            if (
                task.max_running_jobs is not None
                and task.running_jobs >= task.max_running_jobs
            ):
                self._logger.debug(
                    "Skipping job %s because task %r has the maximum number of %d jobs "
                    "already running",
                    job.id,
                    job.task_id,
                    task.running_jobs,
                )
                continue

            # Mark the job as acquired by this worker
            jobs.append(job)
            job.acquired_by = scheduler_id
            job.acquired_until = acquired_until

            # Increment the number of running jobs for this task
            task.running_jobs += 1

            # Exit the loop if enough jobs have been acquired
            if len(jobs) == limit:
                break

        # Publish the appropriate events
        for job in jobs:
            await self._event_broker.publish(
                JobAcquired.from_job(job, scheduler_id=scheduler_id)
            )

        # Discard the jobs that could not start
        for job, result in job_results.items():
            await self.release_job(scheduler_id, job, result)

        return jobs

    async def release_job(self, scheduler_id: str, job: Job, result: JobResult) -> None:
        # Record the job result
        if result.expires_at > result.finished_at:
            self._job_results[result.job_id] = result

        # Decrement the number of running jobs for this task
        if job.acquired_by:
            self._tasks[job.task_id].running_jobs -= 1

        # Delete the job
        job = self._jobs_by_id.pop(result.job_id)

        # Remove the job from the jobs belonging to its task
        task_jobs = self._jobs_by_task_id[job.task_id]
        task_jobs.remove(job)
        if not task_jobs:
            del self._jobs_by_task_id[job.task_id]

        # If this was a scheduled job, remove the job from the set of jobs belonging to
        # this schedule
        if job.schedule_id:
            schedule_jobs = self._jobs_by_schedule_id[job.schedule_id]
            schedule_jobs.remove(job)
            if not schedule_jobs:
                del self._jobs_by_schedule_id[job.schedule_id]

        # Notify other schedulers
        await self._event_broker.publish(
            JobReleased.from_result(
                result,
                scheduler_id,
                job.task_id,
                job.schedule_id,
                job.scheduled_fire_time,
            )
        )

    async def get_job_result(self, job_id: UUID) -> JobResult | None:
        return self._job_results.pop(job_id, None)

    async def extend_acquired_schedule_leases(
        self, scheduler_id: str, schedule_ids: set[str], duration: timedelta
    ) -> None:
        acquired_until = datetime.now(timezone.utc) + duration
        for schedule in self._schedules:
            if schedule.acquired_by == scheduler_id and schedule.id in schedule_ids:
                schedule.acquired_until = acquired_until

    async def extend_acquired_job_leases(
        self, scheduler_id: str, job_ids: set[UUID], duration: timedelta
    ) -> None:
        acquired_until = datetime.now(timezone.utc) + duration
        for job in self._jobs_by_id.values():
            if job.acquired_by == scheduler_id and job.id in job_ids:
                job.acquired_until = acquired_until

    async def reap_abandoned_jobs(self, scheduler_id: str) -> None:
        now = datetime.now(timezone.utc)
        for job in list(self._jobs_by_id.values()):
            if job.acquired_by == scheduler_id:
                result = JobResult.from_job(
                    job=job, outcome=JobOutcome.abandoned, finished_at=now
                )
                await self.release_job(job.acquired_by, job, result)

    async def cleanup(self) -> None:
        # Clean up expired job results
        now = datetime.now(timezone.utc)
        expired_job_ids = [
            result.job_id
            for result in self._job_results.values()
            if result.expires_at <= now
        ]
        for job_id in expired_job_ids:
            del self._job_results[job_id]

        # Finish any jobs whose leases have expired
        expired_jobs = [
            job
            for job in self._jobs_by_id.values()
            if job.acquired_until is not None and job.acquired_until < now
        ]
        for job in expired_jobs:
            result = JobResult.from_job(
                job=job, outcome=JobOutcome.abandoned, finished_at=now
            )
            assert job.acquired_by is not None
            await self.release_job(job.acquired_by, job, result)

        # Clean up finished schedules that have no running jobs
        finished_schedule_ids = [
            schedule_id
            for schedule_id, schedule in self._schedules_by_id.items()
            if schedule.next_fire_time is None
            and schedule_id not in self._jobs_by_schedule_id
        ]
        await self.remove_schedules(finished_schedule_ids, finished=True)
