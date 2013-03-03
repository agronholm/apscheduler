"""
Basic example showing how to start the scheduler and schedule a job that executes on 3 second intervals.
"""

from datetime import datetime

from apscheduler.scheduler import Scheduler


def tick():
    print('Tick! The time is: %s' % datetime.now())


if __name__ == '__main__':
    scheduler = Scheduler(standalone=True)
    scheduler.add_interval_job(tick, seconds=3)
    print('Press Ctrl+C to exit')
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
