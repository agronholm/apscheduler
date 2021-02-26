from bisect import bisect_left, insort_right
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import MAXYEAR, datetime, timezone
from functools import partial
from typing import Dict, Iterable, List, Optional, Set
from uuid import UUID

from anyio import create_event, move_on_after
from anyio.abc import Event

from ..abc import DataStore, Job, Schedule
from ..events import EventHub, ScheduleAdded, ScheduleEvent, ScheduleRemoved, ScheduleUpdated
from ..exceptions import ConflictingIdError
from ..policies import ConflictPolicy

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
        return self.schedule == other.schedule

    def __hash__(self):
        return hash(self.schedule)

    def __repr__(self):
        return (f'<{self.__class__.__name__} id={self.schedule.id!r} '
                f'next_fire_time={self.next_fire_time}>')


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
        return f'<{self.__class__.__name__} id={self.job.id!r} task_id={self.job.task_id!r}'


@dataclass(repr=False)
class MemoryDataStore(DataStore, EventHub):
    lock_expiration_delay: float = 30
    _schedules: List[ScheduleState] = field(init=False, default_factory=list)
    _schedules_by_id: Dict[str, ScheduleState] = field(init=False, default_factory=dict)
    _schedules_by_task_id: Dict[str, Set[ScheduleState]] = field(
        init=False, default_factory=partial(defaultdict, set))
    _schedules_event: Optional[Event] = field(init=False, default=None)
    _jobs: List[JobState] = field(init=False, default_factory=list)
    _jobs_by_id: Dict[UUID, JobState] = field(init=False, default_factory=dict)
    _jobs_by_task_id: Dict[str, Set[JobState]] = field(init=False,
                                                       default_factory=partial(defaultdict, set))
    _jobs_event: Optional[Event] = field(init=False, default=None)
    _loans: int = 0

    def _find_schedule_index(self, state: ScheduleState) -> Optional[int]:
        left_index = bisect_left(self._schedules, state)
        right_index = bisect_left(self._schedules, state)
        return self._schedules.index(state, left_index, right_index + 1)

    def _find_job_index(self, state: JobState) -> Optional[int]:
        left_index = bisect_left(self._jobs, state)
        right_index = bisect_left(self._jobs, state)
        return self._jobs.index(state, left_index, right_index + 1)

    async def __aenter__(self):
        self._loans += 1
        if self._loans == 1:
            self._schedules_event = create_event()
            self._jobs_event = create_event()

        return await super().__aenter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._loans -= 1
        if self._loans == 0:
            self._schedules_event = None
            self._jobs_event = None

        return await super().__aexit__(exc_type, exc_val, exc_tb)

    async def clear(self) -> None:
        self._schedules.clear()
        self._schedules_by_id.clear()
        self._schedules_by_task_id.clear()
        self._jobs.clear()
        self._jobs_by_id.clear()
        self._jobs_by_task_id.clear()

    async def get_schedules(self, ids: Optional[Set[str]] = None) -> List[Schedule]:
        return [state.schedule for state in self._schedules
                if ids is None or state.schedule.id in ids]

    async def add_schedule(self, schedule: Schedule, conflict_policy: ConflictPolicy) -> None:
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
            event = ScheduleUpdated(datetime.now(timezone.utc), schedule.id,
                                    schedule.next_fire_time)
        else:
            event = ScheduleAdded(datetime.now(timezone.utc), schedule.id,
                                  schedule.next_fire_time)

        await self.publish(event)

        old_event, self._schedules_event = self._schedules_event, create_event()
        await old_event.set()

    async def remove_schedules(self, ids: Iterable[str]) -> None:
        events: List[ScheduleRemoved] = []
        for schedule_id in ids:
            state = self._schedules_by_id.pop(schedule_id, None)
            if state:
                self._schedules.remove(state)
                events.append(ScheduleRemoved(datetime.now(timezone.utc), state.schedule.id))

        for event in events:
            await self.publish(event)

    async def acquire_schedules(self, scheduler_id: str, limit: int) -> List[Schedule]:
        while True:
            now = datetime.now(timezone.utc)
            schedules: List[Schedule] = []
            wait_time = None
            for state in self._schedules:
                if state.acquired_by is not None and state.acquired_until >= now:
                    continue
                elif state.next_fire_time is None:
                    break
                elif state.next_fire_time > now:
                    wait_time = state.next_fire_time.timestamp() - now.timestamp()
                    break

                schedules.append(state.schedule)
                state.acquired_by = scheduler_id
                state.acquired_until = datetime.fromtimestamp(
                    now.timestamp() + self.lock_expiration_delay, now.tzinfo)
                if len(schedules) == limit:
                    break

            if schedules:
                return schedules

            # Wait until reaching the next known fire time, or a schedule is added or updated
            async with move_on_after(wait_time):
                await self._schedules_event.wait()

    async def release_schedules(self, scheduler_id: str, schedules: List[Schedule]) -> None:
        # Send update events for schedules that have a next time
        now = datetime.now(timezone.utc)
        update_events: List[ScheduleEvent] = []
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
                update_events.append(ScheduleUpdated(now, s.id, s.next_fire_time))
            else:
                finished_schedule_ids.append(s.id)

        for event in update_events:
            await self.publish(event)

        # Remove schedules that didn't get a new next fire time
        await self.remove_schedules(finished_schedule_ids)

    async def add_job(self, job: Job) -> None:
        state = JobState(job)
        self._jobs.append(state)
        self._jobs_by_id[job.id] = state
        self._jobs_by_task_id[job.task_id].add(state)

        old_event, self._jobs_event = self._jobs_event, create_event()
        await old_event.set()

    async def get_jobs(self, ids: Optional[Iterable[UUID]] = None) -> List[Job]:
        if ids is not None:
            ids = frozenset(ids)

        return [state.job for state in self._jobs if ids is None or state.job.id in ids]

    async def acquire_jobs(self, worker_id: str, limit: Optional[int] = None) -> List[Job]:
        while True:
            now = datetime.now(timezone.utc)
            jobs: List[Job] = []
            for state in self._jobs:
                if state.acquired_by is not None and state.acquired_until >= now:
                    continue

                jobs.append(state.job)
                state.acquired_by = worker_id
                state.acquired_until = datetime.fromtimestamp(
                    now.timestamp() + self.lock_expiration_delay, now.tzinfo)
                if len(jobs) == limit:
                    break

            if jobs:
                return jobs

            # Wait until a new job is added
            await self._jobs_event.wait()

    async def release_jobs(self, worker_id: str, jobs: List[Job]) -> None:
        assert self._jobs
        indexes = sorted((self._find_job_index(self._jobs_by_id[j.id]) for j in jobs),
                         reverse=True)
        for i in indexes:
            state = self._jobs.pop(i)
            self._jobs_by_task_id[state.job.task_id].remove(state)
