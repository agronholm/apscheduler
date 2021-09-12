import threading
import time
from datetime import datetime, timezone

import anyio
import pytest
from anyio import fail_after

from apscheduler.enums import JobOutcome
from apscheduler.events import (
    Event, JobAdded, ScheduleAdded, ScheduleRemoved, SchedulerStarted, SchedulerStopped, TaskAdded)
from apscheduler.exceptions import JobLookupError
from apscheduler.schedulers.async_ import AsyncScheduler
from apscheduler.schedulers.sync import Scheduler
from apscheduler.triggers.date import DateTrigger

pytestmark = pytest.mark.anyio


async def dummy_async_job(delay: float = 0, fail: bool = False) -> str:
    await anyio.sleep(delay)
    if fail:
        raise RuntimeError('failing as requested')
    else:
        return 'returnvalue'


def dummy_sync_job(delay: float = 0, fail: bool = False) -> str:
    time.sleep(delay)
    if fail:
        raise RuntimeError('failing as requested')
    else:
        return 'returnvalue'


class TestAsyncScheduler:
    async def test_schedule_job(self) -> None:
        def listener(received_event: Event) -> None:
            received_events.append(received_event)
            if len(received_events) == 5:
                event.set()

        received_events: list[Event] = []
        event = anyio.Event()
        scheduler = AsyncScheduler(start_worker=False)
        scheduler.events.subscribe(listener)
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

    async def test_get_job_result_success(self) -> None:
        async with AsyncScheduler() as scheduler:
            job_id = await scheduler.add_job(dummy_async_job, kwargs={'delay': 0.2})
            result = await scheduler.get_job_result(job_id)
            assert result.job_id == job_id
            assert result.outcome is JobOutcome.success
            assert result.return_value == 'returnvalue'

    async def test_get_job_result_error(self) -> None:
        async with AsyncScheduler() as scheduler:
            job_id = await scheduler.add_job(dummy_async_job, kwargs={'delay': 0.2, 'fail': True})
            result = await scheduler.get_job_result(job_id)
            assert result.job_id == job_id
            assert result.outcome is JobOutcome.error
            assert isinstance(result.exception, RuntimeError)
            assert str(result.exception) == 'failing as requested'

    async def test_get_job_result_nowait_not_yet_ready(self) -> None:
        async with AsyncScheduler() as scheduler:
            job_id = await scheduler.add_job(dummy_async_job, kwargs={'delay': 0.2})
            with pytest.raises(JobLookupError):
                await scheduler.get_job_result(job_id, wait=False)

    async def test_run_job_success(self) -> None:
        async with AsyncScheduler() as scheduler:
            return_value = await scheduler.run_job(dummy_async_job)
            assert return_value == 'returnvalue'

    async def test_run_job_failure(self) -> None:
        async with AsyncScheduler() as scheduler:
            with pytest.raises(RuntimeError, match='failing as requested'):
                await scheduler.run_job(dummy_async_job, kwargs={'fail': True})


class TestSyncScheduler:
    def test_schedule_job(self):
        def listener(received_event: Event) -> None:
            received_events.append(received_event)
            if len(received_events) == 5:
                event.set()

        received_events: list[Event] = []
        event = threading.Event()
        scheduler = Scheduler(start_worker=False)
        scheduler.events.subscribe(listener)
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

    def test_get_job_result(self) -> None:
        with Scheduler() as scheduler:
            job_id = scheduler.add_job(dummy_sync_job)
            result = scheduler.get_job_result(job_id)
            assert result.outcome is JobOutcome.success
            assert result.return_value == 'returnvalue'

    def test_get_job_result_error(self) -> None:
        with Scheduler() as scheduler:
            job_id = scheduler.add_job(dummy_sync_job, kwargs={'delay': 0.2, 'fail': True})
            result = scheduler.get_job_result(job_id)
            assert result.job_id == job_id
            assert result.outcome is JobOutcome.error
            assert isinstance(result.exception, RuntimeError)
            assert str(result.exception) == 'failing as requested'

    def test_get_job_result_nowait_not_yet_ready(self) -> None:
        with Scheduler() as scheduler:
            job_id = scheduler.add_job(dummy_sync_job, kwargs={'delay': 0.2})
            with pytest.raises(JobLookupError):
                scheduler.get_job_result(job_id, wait=False)

    def test_run_job_success(self) -> None:
        with Scheduler() as scheduler:
            return_value = scheduler.run_job(dummy_sync_job)
            assert return_value == 'returnvalue'

    def test_run_job_failure(self) -> None:
        with Scheduler() as scheduler:
            with pytest.raises(RuntimeError, match='failing as requested'):
                scheduler.run_job(dummy_sync_job, kwargs={'fail': True})
