from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import MAXYEAR, datetime, timezone
from typing import Any, AsyncGenerator, Callable, Dict, Iterable, List, Mapping, Optional, Set

from apscheduler.abc import DataStore, Event, EventHub, Schedule
from apscheduler.eventhubs.local import LocalEventHub
from apscheduler.events import DataStoreEvent, SchedulesAdded, SchedulesRemoved, SchedulesUpdated
from sortedcontainers import SortedSet  # type: ignore

max_datetime = datetime(MAXYEAR, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc)


@dataclass(frozen=True, repr=False)
class MemoryScheduleStore(DataStore):
    _event_hub: EventHub = field(init=False, default_factory=LocalEventHub)
    _schedules: Set[Schedule] = field(
        init=False,
        default_factory=lambda: SortedSet(key=lambda x: x.next_fire_time or max_datetime))
    _schedules_by_id: Dict[str, Schedule] = field(default_factory=dict)

    async def get_schedules(self, ids: Optional[Set[str]] = None) -> List[Schedule]:
        return [sched for sched in self._schedules if ids is None or sched.id in ids]

    async def add_or_replace_schedules(self, schedules: Iterable[Schedule]) -> None:
        added_schedule_ids = set()
        updated_schedule_ids = set()
        for schedule in schedules:
            if schedule.id in self._schedules_by_id:
                updated_schedule_ids.add(schedule.id)
                old_schedule = self._schedules_by_id[schedule.id]
                self._schedules.remove(old_schedule)
            else:
                added_schedule_ids.add(schedule.id)

            self._schedules_by_id[schedule.id] = schedule
            self._schedules.add(schedule)

        event: DataStoreEvent
        if added_schedule_ids:
            earliest_fire_time = min(
                (schedule.next_fire_time for schedule in schedules
                 if schedule.id in added_schedule_ids and schedule.next_fire_time),
                default=None)
            event = SchedulesAdded(datetime.now(timezone.utc), added_schedule_ids,
                                   earliest_fire_time)
            await self._event_hub.publish(event)

        if updated_schedule_ids:
            earliest_fire_time = min(
                (schedule.next_fire_time for schedule in schedules
                 if schedule.id in updated_schedule_ids and schedule.next_fire_time),
                default=None)
            event = SchedulesUpdated(datetime.now(timezone.utc), updated_schedule_ids,
                                     earliest_fire_time)
            await self._event_hub.publish(event)

    async def update_schedules(self, updates: Mapping[str, Dict[str, Any]]) -> Set[str]:
        updated_schedule_ids: Set[str] = set()
        earliest_fire_time: Optional[datetime] = None
        for schedule_id, changes in updates.items():
            try:
                schedule = self._schedules_by_id[schedule_id]
            except KeyError:
                continue

            if changes:
                updated_schedule_ids.add(schedule.id)
                for key, value in changes.items():
                    setattr(schedule, key, value)
                    if key == 'next_fire_time':
                        if not earliest_fire_time:
                            earliest_fire_time = schedule.next_fire_time
                        elif (schedule.next_fire_time
                              and schedule.next_fire_time < earliest_fire_time):
                            earliest_fire_time = schedule.next_fire_time

        event = SchedulesUpdated(datetime.now(timezone.utc), updated_schedule_ids,
                                 earliest_fire_time)
        await self._event_hub.publish(event)

        return updated_schedule_ids

    async def remove_schedules(self, ids: Optional[Set[str]] = None) -> None:
        if ids is None:
            removed_schedule_ids = set(self._schedules_by_id)
            self._schedules_by_id.clear()
            self._schedules.clear()
        else:
            removed_schedule_ids = set()
            for schedule_id in ids:
                schedule = self._schedules_by_id.pop(schedule_id, None)
                if schedule:
                    removed_schedule_ids.add(schedule.id)
                    self._schedules.remove(schedule)

        event = SchedulesRemoved(datetime.now(timezone.utc), removed_schedule_ids)
        await self._event_hub.publish(event)

    @asynccontextmanager
    async def acquire_due_schedules(
            self, scheduler_id: str,
            max_scheduled_time: datetime) -> AsyncGenerator[List[Schedule], None]:
        pending: List[Schedule] = []
        for schedule in self._schedules:
            if not schedule.next_fire_time or schedule.next_fire_time > max_scheduled_time:
                break

            pending.append(schedule)

        yield pending

    async def get_next_fire_time(self) -> Optional[datetime]:
        for schedule in self._schedules:
            return schedule.next_fire_time

        return None

    async def subscribe(self, callback: Callable[[Event], Any]) -> None:
        await self._event_hub.subscribe(callback)
