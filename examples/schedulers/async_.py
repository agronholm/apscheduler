import logging

import anyio
from apscheduler.schedulers.async_ import AsyncScheduler
from apscheduler.triggers.interval import IntervalTrigger


def say_hello():
    print('Hello!')


async def main():
    async with AsyncScheduler() as scheduler:
        await scheduler.add_schedule(say_hello, IntervalTrigger(seconds=1))

logging.basicConfig(level=logging.DEBUG)
try:
    anyio.run(main)
except (KeyboardInterrupt, SystemExit):
    pass
