from __future__ import annotations

import logging

import anyio

from apscheduler.schedulers.async_ import AsyncScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.workers.async_ import AsyncWorker


def say_hello():
    print("Hello!")


async def main():
    async with AsyncScheduler() as scheduler, AsyncWorker(scheduler.data_store):
        await scheduler.add_schedule(say_hello, IntervalTrigger(seconds=1))
        await scheduler.wait_until_stopped()


logging.basicConfig(level=logging.DEBUG)
try:
    anyio.run(main)
except (KeyboardInterrupt, SystemExit):
    pass
