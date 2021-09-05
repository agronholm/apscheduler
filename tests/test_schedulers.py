import threading
from datetime import datetime, timezone
from typing import List

import anyio
import pytest
from anyio import fail_after

from apscheduler.events import (
    Event, JobAdded, ScheduleAdded, ScheduleRemoved, SchedulerStarted, SchedulerStopped, TaskAdded)
from apscheduler.schedulers.async_ import AsyncScheduler
from apscheduler.schedulers.sync import Scheduler
from apscheduler.triggers.date import DateTrigger

pytestmark = pytest.mark.anyio


async def dummy_async_job():
    return 'returnvalue'


def dummy_sync_job():
    return 'returnvalue'


class TestAsyncScheduler:
    async def test_schedule_job(self) -> None:
        def listener(received_event: Event) -> None:
            received_events.append(received_event)
            if len(received_events) == 5:
                event.set()

        received_events: List[Event] = []
        event = anyio.Event()
        scheduler = AsyncScheduler(start_worker=False)
        scheduler.subscribe(listener)
        trigger = DateTrigger(datetime.now(timezone.utc))
        async with scheduler:
            await scheduler.add_schedule(dummy_async_job, trigger, id='foo')
            with fail_after(3):
                await event.wait()

        # The scheduler was first started
        received_event = received_events.pop(0)
        assert isinstance(received_event, SchedulerStarted)

        # Then the task was added
        received_event = received_events.pop(0)
        assert isinstance(received_event, TaskAdded)
        assert received_event.task_id == 'test_schedulers:dummy_async_job'

        # Then a schedule was added
        received_event = received_events.pop(0)
        assert isinstance(received_event, ScheduleAdded)
        assert received_event.schedule_id == 'foo'
        # assert received_event.task_id == 'task_id'

        # Then that schedule was processed and a job was added for it
        received_event = received_events.pop(0)
        assert isinstance(received_event, JobAdded)
        assert received_event.schedule_id == 'foo'
        assert received_event.task_id == 'test_schedulers:dummy_async_job'

        # Then the schedule was removed since the trigger had been exhausted
        received_event = received_events.pop(0)
        assert isinstance(received_event, ScheduleRemoved)
        assert received_event.schedule_id == 'foo'

        # Finally, the scheduler was stopped
        received_event = received_events.pop(0)
        assert isinstance(received_event, SchedulerStopped)

        # There should be no more events on the list
        assert not received_events


class TestSyncScheduler:
    def test_schedule_job(self):
        def listener(received_event: Event) -> None:
            received_events.append(received_event)
            if len(received_events) == 5:
                event.set()

        received_events: List[Event] = []
        event = threading.Event()
        scheduler = Scheduler(start_worker=False)
        scheduler.subscribe(listener)
        trigger = DateTrigger(datetime.now(timezone.utc))
        with scheduler:
            scheduler.add_schedule(dummy_sync_job, trigger, id='foo')
            event.wait(3)

        # The scheduler was first started
        received_event = received_events.pop(0)
        assert isinstance(received_event, SchedulerStarted)

        # Then the task was added
        received_event = received_events.pop(0)
        assert isinstance(received_event, TaskAdded)
        assert received_event.task_id == 'test_schedulers:dummy_sync_job'

        # Then a schedule was added
        received_event = received_events.pop(0)
        assert isinstance(received_event, ScheduleAdded)
        assert received_event.schedule_id == 'foo'
        # assert received_event.task_id == 'task_id'

        # Then that schedule was processed and a job was added for it
        received_event = received_events.pop(0)
        assert isinstance(received_event, JobAdded)
        assert received_event.schedule_id == 'foo'
        assert received_event.task_id == 'test_schedulers:dummy_sync_job'

        # Then the schedule was removed since the trigger had been exhausted
        received_event = received_events.pop(0)
        assert isinstance(received_event, ScheduleRemoved)
        assert received_event.schedule_id == 'foo'

        # Finally, the scheduler was stopped
        received_event = received_events.pop(0)
        assert isinstance(received_event, SchedulerStopped)

        # There should be no more events on the list
        assert not received_events
