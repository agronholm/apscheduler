"""
This is an example demonstrating the use of the scheduler as only an interface to the
scheduling system. This script adds or updates a single schedule and then exits. To see
the schedule acted on, you need to run the corresponding worker script (either
async_worker.py or sync_worker.py).

This script requires the "postgresql" service to be running.
To install prerequisites: pip install sqlalchemy asyncpg
To run: python async_scheduler.py
"""

from __future__ import annotations

import asyncio
import logging

from example_tasks import tick
from sqlalchemy.ext.asyncio import create_async_engine

from apscheduler import AsyncScheduler
from apscheduler.datastores.sqlalchemy import SQLAlchemyDataStore
from apscheduler.eventbrokers.asyncpg import AsyncpgEventBroker
from apscheduler.triggers.interval import IntervalTrigger


async def main():
    engine = create_async_engine(
        "postgresql+asyncpg://postgres:secret@localhost/testdb"
    )
    data_store = SQLAlchemyDataStore(engine)
    event_broker = AsyncpgEventBroker.from_async_sqla_engine(engine)

    # Uncomment the next two lines to use the Redis event broker instead
    # from apscheduler.eventbrokers.redis import RedisEventBroker
    # event_broker = RedisEventBroker.from_url("redis://localhost")

    async with AsyncScheduler(data_store, event_broker) as scheduler:
        await scheduler.add_schedule(tick, IntervalTrigger(seconds=1), id="tick")
        # Note: we don't actually start the scheduler here!


logging.basicConfig(level=logging.INFO)
asyncio.run(main())
