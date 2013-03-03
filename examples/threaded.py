"""
Basic example showing how the scheduler integrates with the application it's running alongside with.
"""

from datetime import datetime
import time

from apscheduler.scheduler import Scheduler


def tick():
    print('Tick! The time is: %s' % datetime.now())


if __name__ == '__main__':
    scheduler = Scheduler()
    scheduler.add_interval_job(tick, seconds=3)
    print('Press Ctrl+C to exit')
    scheduler.start()

    # This is here to simulate application activity (which keeps the main
    # thread alive).
    while True:
        print('This is the main thread.')
        time.sleep(2)
