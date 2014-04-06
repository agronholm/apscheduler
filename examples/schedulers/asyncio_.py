"""
Demonstrates how to use the Tornado compatible scheduler to schedule a job that executes on 3 second intervals.
"""

from datetime import datetime
import asyncio

from apscheduler.schedulers.asyncio import AsyncIOScheduler


def tick():
    print('Tick! The time is: %s' % datetime.now())


if __name__ == '__main__':
    scheduler = AsyncIOScheduler()
    scheduler.add_job(tick, 'interval', seconds=3)
    scheduler.start()
    print('Press Ctrl+C to exit')

    # Execution will block here until Ctrl+C is pressed.
    try:
        asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt, SystemExit):
        pass
