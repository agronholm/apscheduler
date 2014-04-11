"""
Demonstrates how to schedule a job to be run in a process pool on 3 second intervals.
"""

from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import PoolExecutor


def tick():
    print('Tick! The time is: %s' % datetime.now())


if __name__ == '__main__':
    scheduler = BlockingScheduler()
    scheduler.add_executor(PoolExecutor('process'))
    scheduler.add_job(tick, 'interval', seconds=3)
    print('Press Ctrl+C to exit')

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
