import logging

from apscheduler.schedulers.sync import SyncScheduler
from apscheduler.triggers.interval import IntervalTrigger


def say_hello():
    print('Hello!')


logging.basicConfig(level=logging.DEBUG)
try:
    with SyncScheduler() as scheduler:
        scheduler.add_schedule(say_hello, IntervalTrigger(seconds=1))
except (KeyboardInterrupt, SystemExit):
    pass
