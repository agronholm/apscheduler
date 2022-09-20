"""
Example demonstrating the separation of scheduler and worker.
This script runs the scheduler part. You need to be running both this and the worker
script simultaneously in order for the scheduled task to be run.

Requires the "postgresql" service to be running.
To install prerequisites: pip install sqlalchemy asyncpg
To run: python async_worker.py

When run together with async_scheduler.py, it should print a line on the console
on a one-second interval.
"""

from __future__ import annotations

import asyncio
import logging

from sqlalchemy.ext.asyncio import create_async_engine

from apscheduler import SchedulerRole
from apscheduler.datastores.sqlalchemy import SQLAlchemyDataStore
from apscheduler.eventbrokers.asyncpg import AsyncpgEventBroker
from apscheduler.schedulers.async_ import AsyncScheduler


async def main():
    engine = create_async_engine(
        "postgresql+asyncpg://postgres:secret@localhost/testdb"
    )
    data_store = SQLAlchemyDataStore(engine)
    event_broker = AsyncpgEventBroker.from_async_sqla_engine(engine)

    # Uncomment the next two lines to use the Redis event broker instead
    # from apscheduler.eventbrokers.redis import RedisEventBroker
    # event_broker = RedisEventBroker.from_url("redis://localhost")

    scheduler = AsyncScheduler(data_store, event_broker, role=SchedulerRole.worker)
    await scheduler.run_until_stopped()


logging.basicConfig(level=logging.INFO)
asyncio.run(main())
