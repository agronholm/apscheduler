import logging
from datetime import datetime, timezone

import pytest
from apscheduler.events import JobSuccessful
from apscheduler.schedulers.async_ import AsyncScheduler
from apscheduler.schedulers.sync import SyncScheduler
from apscheduler.triggers.date import DateTrigger

pytestmark = pytest.mark.anyio


async def dummy_async_job():
    return 'returnvalue'


def dummy_sync_job():
    return 'returnvalue'


class TestAsyncScheduler:
    async def test_schedule_job(self, caplog, store):
        async def listener(event):
            events.append(event)
            if isinstance(event, JobSuccessful):
                await scheduler.stop()

        caplog.set_level(logging.DEBUG)
        trigger = DateTrigger(datetime.now(timezone.utc))
        events = []
        async with AsyncScheduler(store) as scheduler:
            scheduler.worker.subscribe(listener)
            await scheduler.add_schedule(dummy_async_job, trigger)
            await scheduler.wait_until_stopped()

        assert len(events) == 2
        assert isinstance(events[1], JobSuccessful)
        assert events[1].return_value == 'returnvalue'


class TestSyncScheduler:
    @pytest.mark.parametrize('anyio_backend', ['asyncio'])
    def test_schedule_job(self, caplog, anyio_backend, sync_store, portal):
        def listener(event):
            events.append(event)
            if isinstance(event, JobSuccessful):
                scheduler.stop()

        caplog.set_level(logging.DEBUG)
        events = []
        with SyncScheduler(sync_store, portal=portal) as scheduler:
            scheduler.worker.subscribe(listener)
            scheduler.add_schedule(dummy_sync_job, DateTrigger(datetime.now(timezone.utc)))
            scheduler.wait_until_stopped()

        assert len(events) == 2
        assert isinstance(events[1], JobSuccessful)
        assert events[1].return_value == 'returnvalue'
