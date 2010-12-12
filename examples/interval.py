"""
Basic example showing how to start the scheduler and schedule a job that
executes on 3 second intervals. It uses sys.stdout.write instead of print to
allow it to work unmodified on both Python 2.x and 3.x.
"""

from datetime import datetime
import sys
import time

from apscheduler.scheduler import Scheduler


def tick():
    sys.stdout.write('Tick! The time is: %s\n' % datetime.now())


if __name__ == '__main__':
    scheduler = Scheduler()
    scheduler.add_interval_job(tick, seconds=3)
    sys.stdout.write('Press Ctrl+C to exit\n')
    scheduler.start()

    # This is here to prevent the main thread from exiting so that the
    # scheduler has time to work -- this should not be necessary in real world
    # applications
    time.sleep(9999)
