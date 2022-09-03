"""
Example demonstrating the separation of scheduler and worker.
This script runs the scheduler part. You need to be running both this and the worker
script simultaneously in order for the scheduled task to be run.

Requires the "postgresql" service to be running.
To install prerequisites: pip install sqlalchemy asyncpg
To run: python async_scheduler.py

When run together with async_worker.py, it should print a line on the console
on a one-second interval.
"""

from __future__ import annotations

import asyncio
import logging

from example_tasks import tick
from sqlalchemy.ext.asyncio import create_async_engine

from apscheduler.datastores.async_sqlalchemy import AsyncSQLAlchemyDataStore
from apscheduler.eventbrokers.asyncpg import AsyncpgEventBroker
from apscheduler.schedulers.async_ import AsyncScheduler
from apscheduler.triggers.interval import IntervalTrigger


async def main():
    engine = create_async_engine(
        "postgresql+asyncpg://postgres:secret@localhost/testdb"
    )
    data_store = AsyncSQLAlchemyDataStore(engine)
    event_broker = AsyncpgEventBroker.from_async_sqla_engine(engine)

    # Uncomment the next two lines to use the Redis event broker instead
    # from apscheduler.eventbrokers.async_redis import AsyncRedisEventBroker
    # event_broker = AsyncRedisEventBroker.from_url("redis://localhost")

    async with AsyncScheduler(
        data_store, event_broker, start_worker=False
    ) as scheduler:
        await scheduler.add_schedule(tick, IntervalTrigger(seconds=1), id="tick")
        await scheduler.run_until_stopped()


logging.basicConfig(level=logging.INFO)
asyncio.run(main())
