"""
Example demonstrating use of the asynchronous scheduler with persistence via PostgreSQL
in a simple asyncio app.

Requires the "postgresql" service to be running.
To install prerequisites: pip install sqlalchemy asyncpg
To run: python async_postgres.py

It should print a line on the console on a one-second interval.
"""

from __future__ import annotations

from asyncio import run
from datetime import datetime

from sqlalchemy.ext.asyncio import create_async_engine

from apscheduler.datastores.async_sqlalchemy import AsyncSQLAlchemyDataStore
from apscheduler.schedulers.async_ import AsyncScheduler
from apscheduler.triggers.interval import IntervalTrigger


def tick():
    print("Hello, the time is", datetime.now())


async def main():
    engine = create_async_engine(
        "postgresql+asyncpg://postgres:secret@localhost/testdb"
    )
    data_store = AsyncSQLAlchemyDataStore(engine)
    async with AsyncScheduler(data_store) as scheduler:
        await scheduler.add_schedule(tick, IntervalTrigger(seconds=1), id="tick")
        await scheduler.run_until_stopped()


run(main())
