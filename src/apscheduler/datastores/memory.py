from __future__ import annotations

from bisect import bisect_left, insort_right
from collections import defaultdict
from datetime import MAXYEAR, datetime, timedelta, timezone
from functools import partial
from typing import Any, Iterable
from uuid import UUID

import attrs

from .._enums import ConflictPolicy
from .._events import (
    JobAcquired,
    JobAdded,
    ScheduleAdded,
    ScheduleRemoved,
    ScheduleUpdated,
    TaskAdded,
    TaskRemoved,
    TaskUpdated,
)
from .._exceptions import ConflictingIdError, TaskLookupError
from .._structures import Job, JobResult, Schedule, Task
from .base import BaseDataStore

max_datetime = datetime(MAXYEAR, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc)


@attrs.define
class TaskState:
    task: Task
    running_jobs: int = 0
    saved_state: Any = None

    def __eq__(self, other):
        return self.task.id == other.task.id


@attrs.define
class ScheduleState:
    schedule: Schedule
    next_fire_time: datetime | None = attrs.field(init=False, eq=False)
    acquired_by: str | None = attrs.field(init=False, eq=False, default=None)
    acquired_until: datetime | None = attrs.field(init=False, eq=False, default=None)

    def __attrs_post_init__(self):
        self.next_fire_time = self.schedule.next_fire_time

    def __eq__(self, other):
        return self.schedule.id == other.schedule.id

    def __lt__(self, other):
        if self.next_fire_time is None:
            return False
        elif other.next_fire_time is None:
            return self.next_fire_time is not None
        elif self.next_fire_time != other.next_fire_time:
            return self.next_fire_time < other.next_fire_time
        else:
            return self.schedule.id < other.schedule.id

    def __hash__(self):
        return hash(self.schedule.id)


@attrs.define(order=True)
class JobState:
    job: Job = attrs.field(order=False)
    created_at: datetime = attrs.field(
        init=False, factory=partial(datetime.now, timezone.utc)
    )
    acquired_by: str | None = attrs.field(eq=False, order=False, default=None)
    acquired_until: datetime | None = attrs.field(eq=False, order=False, default=None)

    def __eq__(self, other):
        return self.job.id == other.job.id

    def __hash__(self):
        return hash(self.job.id)


@attrs.define(eq=False)
class MemoryDataStore(BaseDataStore):
    """
    Stores scheduler data in memory, without serializing it.

    Can be shared between multiple schedulers and workers within the same event loop.

    :param lock_expiration_delay: maximum amount of time (in seconds) that a scheduler
        or worker can keep a lock on a schedule or task
    """

    lock_expiration_delay: float = 30
    _tasks: dict[str, TaskState] = attrs.Factory(dict)
    _schedules: list[ScheduleState] = attrs.Factory(list)
    _schedules_by_id: dict[str, ScheduleState] = attrs.Factory(dict)
    _schedules_by_task_id: dict[str, set[ScheduleState]] = attrs.Factory(
        partial(defaultdict, set)
    )
    _jobs: list[JobState] = attrs.Factory(list)
    _jobs_by_id: dict[UUID, JobState] = attrs.Factory(dict)
    _jobs_by_task_id: dict[str, set[JobState]] = attrs.Factory(
        partial(defaultdict, set)
    )
    _job_results: dict[UUID, JobResult] = attrs.Factory(dict)

    def _find_schedule_index(self, state: ScheduleState) -> int | None:
        left_index = bisect_left(self._schedules, state)
        right_index = bisect_left(self._schedules, state)
        return self._schedules.index(state, left_index, right_index + 1)

    def _find_job_index(self, state: JobState) -> int | None:
        left_index = bisect_left(self._jobs, state)
        right_index = bisect_left(self._jobs, state)
        return self._jobs.index(state, left_index, right_index + 1)

    def get_schedules(self, ids: set[str] | None = None) -> list[Schedule]:
        return [
            state.schedule
            for state in self._schedules
            if ids is None or state.schedule.id in ids
        ]

    def add_task(self, task: Task) -> None:
        task_exists = task.id in self._tasks
        self._tasks[task.id] = TaskState(task)
        if task_exists:
            self._events.publish(TaskUpdated(task_id=task.id))
        else:
            self._events.publish(TaskAdded(task_id=task.id))

    def remove_task(self, task_id: str) -> None:
        try:
            del self._tasks[task_id]
        except KeyError:
            raise TaskLookupError(task_id) from None

        self._events.publish(TaskRemoved(task_id=task_id))

    def get_task(self, task_id: str) -> Task:
        try:
            return self._tasks[task_id].task
        except KeyError:
            raise TaskLookupError(task_id) from None

    def get_tasks(self) -> list[Task]:
        return sorted(
            (state.task for state in self._tasks.values()), key=lambda task: task.id
        )

    def add_schedule(self, schedule: Schedule, conflict_policy: ConflictPolicy) -> None:
        old_state = self._schedules_by_id.get(schedule.id)
        if old_state is not None:
            if conflict_policy is ConflictPolicy.do_nothing:
                return
            elif conflict_policy is ConflictPolicy.exception:
                raise ConflictingIdError(schedule.id)

            index = self._find_schedule_index(old_state)
            del self._schedules[index]
            self._schedules_by_task_id[old_state.schedule.task_id].remove(old_state)

        state = ScheduleState(schedule)
        self._schedules_by_id[schedule.id] = state
        self._schedules_by_task_id[schedule.task_id].add(state)
        insort_right(self._schedules, state)

        if old_state is not None:
            event = ScheduleUpdated(
                schedule_id=schedule.id, next_fire_time=schedule.next_fire_time
            )
        else:
            event = ScheduleAdded(
                schedule_id=schedule.id, next_fire_time=schedule.next_fire_time
            )

        self._events.publish(event)

    def remove_schedules(self, ids: Iterable[str]) -> None:
        for schedule_id in ids:
            state = self._schedules_by_id.pop(schedule_id, None)
            if state:
                self._schedules.remove(state)
                event = ScheduleRemoved(schedule_id=state.schedule.id)
                self._events.publish(event)

    def acquire_schedules(self, scheduler_id: str, limit: int) -> list[Schedule]:
        now = datetime.now(timezone.utc)
        schedules: list[Schedule] = []
        for state in self._schedules:
            if state.next_fire_time is None or state.next_fire_time > now:
                # The schedule is either paused or not yet due
                break
            elif state.acquired_by is not None:
                if state.acquired_by != scheduler_id and now <= state.acquired_until:
                    # The schedule has been acquired by another scheduler and the
                    # timeout has not expired yet
                    continue

            schedules.append(state.schedule)
            state.acquired_by = scheduler_id
            state.acquired_until = now + timedelta(seconds=self.lock_expiration_delay)
            if len(schedules) == limit:
                break

        return schedules

    def release_schedules(self, scheduler_id: str, schedules: list[Schedule]) -> None:
        # Send update events for schedules that have a next time
        finished_schedule_ids: list[str] = []
        for s in schedules:
            if s.next_fire_time is not None:
                # Remove the schedule
                schedule_state = self._schedules_by_id.get(s.id)
                index = self._find_schedule_index(schedule_state)
                del self._schedules[index]

                # Re-add the schedule to its new position
                schedule_state.next_fire_time = s.next_fire_time
                schedule_state.acquired_by = None
                schedule_state.acquired_until = None
                insort_right(self._schedules, schedule_state)
                event = ScheduleUpdated(
                    schedule_id=s.id, next_fire_time=s.next_fire_time
                )
                self._events.publish(event)
            else:
                finished_schedule_ids.append(s.id)

        # Remove schedules that didn't get a new next fire time
        self.remove_schedules(finished_schedule_ids)

    def get_next_schedule_run_time(self) -> datetime | None:
        return self._schedules[0].next_fire_time if self._schedules else None

    def add_job(self, job: Job) -> None:
        state = JobState(job)
        self._jobs.append(state)
        self._jobs_by_id[job.id] = state
        self._jobs_by_task_id[job.task_id].add(state)

        event = JobAdded(
            job_id=job.id,
            task_id=job.task_id,
            schedule_id=job.schedule_id,
            tags=job.tags,
        )
        self._events.publish(event)

    def get_jobs(self, ids: Iterable[UUID] | None = None) -> list[Job]:
        if ids is not None:
            ids = frozenset(ids)

        return [state.job for state in self._jobs if ids is None or state.job.id in ids]

    def acquire_jobs(self, worker_id: str, limit: int | None = None) -> list[Job]:
        now = datetime.now(timezone.utc)
        jobs: list[Job] = []
        for _index, job_state in enumerate(self._jobs):
            task_state = self._tasks[job_state.job.task_id]

            # Skip already acquired jobs (unless the acquisition lock has expired)
            if job_state.acquired_by is not None:
                if job_state.acquired_until >= now:
                    continue
                else:
                    task_state.running_jobs -= 1

            # Check if the task allows one more job to be started
            if (
                task_state.task.max_running_jobs is not None
                and task_state.running_jobs >= task_state.task.max_running_jobs
            ):
                continue

            # Mark the job as acquired by this worker
            jobs.append(job_state.job)
            job_state.acquired_by = worker_id
            job_state.acquired_until = now + timedelta(
                seconds=self.lock_expiration_delay
            )

            # Increment the number of running jobs for this task
            task_state.running_jobs += 1

            # Exit the loop if enough jobs have been acquired
            if len(jobs) == limit:
                break

        # Publish the appropriate events
        for job in jobs:
            self._events.publish(JobAcquired(job_id=job.id, worker_id=worker_id))

        return jobs

    def release_job(self, worker_id: str, task_id: str, result: JobResult) -> None:
        # Record the job result
        if result.expires_at > result.finished_at:
            self._job_results[result.job_id] = result

        # Decrement the number of running jobs for this task
        task_state = self._tasks.get(task_id)
        if task_state is not None:
            task_state.running_jobs -= 1

        # Delete the job
        job_state = self._jobs_by_id.pop(result.job_id)
        self._jobs_by_task_id[task_id].remove(job_state)
        index = self._find_job_index(job_state)
        del self._jobs[index]

    def get_job_result(self, job_id: UUID) -> JobResult | None:
        return self._job_results.pop(job_id, None)
