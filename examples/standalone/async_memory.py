"""
Example demonstrating use of the asynchronous scheduler in a simple asyncio app.

To run: python async_memory.py

It should print a line on the console on a one-second interval.
"""

from __future__ import annotations

from asyncio import run
from datetime import datetime

from apscheduler.schedulers.async_ import AsyncScheduler
from apscheduler.triggers.interval import IntervalTrigger


def tick():
    print("Hello, the time is", datetime.now())


async def main():
    async with AsyncScheduler() as scheduler:
        await scheduler.add_schedule(tick, IntervalTrigger(seconds=1))
        await scheduler.run_until_stopped()


run(main())
