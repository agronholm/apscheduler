from __future__ import annotations

from bisect import bisect_left, insort_right
from collections import defaultdict
from datetime import MAXYEAR, datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Type
from uuid import UUID

from ... import events
from ...abc import DataStore, Job, Schedule
from ...events import (
    EventHub, JobAdded, ScheduleAdded, ScheduleRemoved, ScheduleUpdated, SubscriptionToken)
from ...exceptions import ConflictingIdError
from ...policies import ConflictPolicy
from ...util import reentrant

max_datetime = datetime(MAXYEAR, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc)


class ScheduleState:
    __slots__ = 'schedule', 'next_fire_time', 'acquired_by', 'acquired_until'

    def __init__(self, schedule: Schedule):
        self.schedule = schedule
        self.next_fire_time = self.schedule.next_fire_time
        self.acquired_by: Optional[str] = None
        self.acquired_until: Optional[datetime] = None

    def __lt__(self, other):
        if self.next_fire_time is None:
            return False
        elif other.next_fire_time is None:
            return self.next_fire_time is not None
        else:
            return self.next_fire_time < other.next_fire_time

    def __eq__(self, other):
        if isinstance(other, ScheduleState):
            return self.schedule == other.schedule

        return NotImplemented

    def __hash__(self):
        return hash(self.schedule.id)

    def __repr__(self):
        return (f'<{self.__class__.__name__} id={self.schedule.id!r} '
                f'task_id={self.schedule.task_id!r} next_fire_time={self.next_fire_time}>')


class JobState:
    __slots__ = 'job', 'created_at', 'acquired_by', 'acquired_until'

    def __init__(self, job: Job):
        self.job = job
        self.created_at = datetime.now(timezone.utc)
        self.acquired_by: Optional[str] = None
        self.acquired_until: Optional[datetime] = None

    def __lt__(self, other):
        return self.created_at < other.created_at

    def __eq__(self, other):
        return self.job.id == other.job.id

    def __hash__(self):
        return hash(self.job.id)

    def __repr__(self):
        return f'<{self.__class__.__name__} id={self.job.id!r} task_id={self.job.task_id!r}>'


@reentrant
class MemoryDataStore(DataStore):
    def __init__(self, lock_expiration_delay: float = 30):
        self.lock_expiration_delay = lock_expiration_delay
        self._events = EventHub()
        self._schedules: List[ScheduleState] = []
        self._schedules_by_id: Dict[str, ScheduleState] = {}
        self._schedules_by_task_id: Dict[str, Set[ScheduleState]] = defaultdict(set)
        self._jobs: List[JobState] = []
        self._jobs_by_id: Dict[UUID, JobState] = {}
        self._jobs_by_task_id: Dict[str, Set[JobState]] = defaultdict(set)

    def _find_schedule_index(self, state: ScheduleState) -> Optional[int]:
        left_index = bisect_left(self._schedules, state)
        right_index = bisect_left(self._schedules, state)
        return self._schedules.index(state, left_index, right_index + 1)

    def _find_job_index(self, state: JobState) -> Optional[int]:
        left_index = bisect_left(self._jobs, state)
        right_index = bisect_left(self._jobs, state)
        return self._jobs.index(state, left_index, right_index + 1)

    def __enter__(self):
        self._events.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._events.__exit__(exc_type, exc_val, exc_tb)

    def subscribe(self, callback: Callable[[events.Event], Any],
                  event_types: Optional[Iterable[Type[events.Event]]] = None) -> SubscriptionToken:
        return self._events.subscribe(callback, event_types)

    def unsubscribe(self, token: events.SubscriptionToken) -> None:
        self._events.unsubscribe(token)

    def clear(self) -> None:
        self._schedules.clear()
        self._schedules_by_id.clear()
        self._schedules_by_task_id.clear()
        self._jobs.clear()
        self._jobs_by_id.clear()
        self._jobs_by_task_id.clear()

    def get_schedules(self, ids: Optional[Set[str]] = None) -> List[Schedule]:
        return [state.schedule for state in self._schedules
                if ids is None or state.schedule.id in ids]

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
            event = ScheduleUpdated(schedule_id=schedule.id,
                                    next_fire_time=schedule.next_fire_time)
        else:
            event = ScheduleAdded(schedule_id=schedule.id,
                                  next_fire_time=schedule.next_fire_time)

        self._events.publish(event)

    def remove_schedules(self, ids: Iterable[str]) -> None:
        for schedule_id in ids:
            state = self._schedules_by_id.pop(schedule_id, None)
            if state:
                self._schedules.remove(state)
                event = ScheduleRemoved(schedule_id=state.schedule.id)
                self._events.publish(event)

    def acquire_schedules(self, scheduler_id: str, limit: int) -> List[Schedule]:
        now = datetime.now(timezone.utc)
        schedules: List[Schedule] = []
        for state in self._schedules:
            if state.acquired_by is not None and state.acquired_until >= now:
                continue
            elif state.next_fire_time is None or state.next_fire_time > now:
                break

            schedules.append(state.schedule)
            state.acquired_by = scheduler_id
            state.acquired_until = now + timedelta(seconds=self.lock_expiration_delay)
            if len(schedules) == limit:
                break

        return schedules

    def release_schedules(self, scheduler_id: str, schedules: List[Schedule]) -> None:
        # Send update events for schedules that have a next time
        finished_schedule_ids: List[str] = []
        for s in schedules:
            if s.next_fire_time is not None:
                # Remove the schedule
                schedule_state = self._schedules_by_id.get(s.id)
                index = self._find_schedule_index(schedule_state)
                del self._schedules[index]

                # Readd the schedule to its new position
                schedule_state.next_fire_time = s.next_fire_time
                schedule_state.acquired_by = None
                schedule_state.acquired_until = None
                insort_right(self._schedules, schedule_state)
                event = ScheduleUpdated(schedule_id=s.id, next_fire_time=s.next_fire_time)
                self._events.publish(event)
            else:
                finished_schedule_ids.append(s.id)

        # Remove schedules that didn't get a new next fire time
        self.remove_schedules(finished_schedule_ids)

    def add_job(self, job: Job) -> None:
        state = JobState(job)
        self._jobs.append(state)
        self._jobs_by_id[job.id] = state
        self._jobs_by_task_id[job.task_id].add(state)

        event = JobAdded(job_id=job.id, task_id=job.task_id, schedule_id=job.schedule_id,
                         tags=job.tags)
        self._events.publish(event)

    def get_jobs(self, ids: Optional[Iterable[UUID]] = None) -> List[Job]:
        if ids is not None:
            ids = frozenset(ids)

        return [state.job for state in self._jobs if ids is None or state.job.id in ids]

    def acquire_jobs(self, worker_id: str, limit: Optional[int] = None) -> List[Job]:
        now = datetime.now(timezone.utc)
        jobs: List[Job] = []
        for state in self._jobs:
            if state.acquired_by is not None and state.acquired_until >= now:
                continue

            jobs.append(state.job)
            state.acquired_by = worker_id
            state.acquired_until = now + timedelta(seconds=self.lock_expiration_delay)
            if len(jobs) == limit:
                break

        return jobs

    def release_jobs(self, worker_id: str, jobs: List[Job]) -> None:
        assert self._jobs
        indexes = sorted((self._find_job_index(self._jobs_by_id[j.id]) for j in jobs),
                         reverse=True)
        for i in indexes:
            state = self._jobs.pop(i)
            self._jobs_by_task_id[state.job.task_id].remove(state)
