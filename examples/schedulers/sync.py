import logging

from apscheduler.schedulers.sync import Scheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.workers.sync import Worker


def say_hello():
    print('Hello!')


logging.basicConfig(level=logging.DEBUG)
try:
    with Scheduler() as scheduler, Worker(scheduler.data_store, portal=scheduler.portal):
        scheduler.add_schedule(say_hello, IntervalTrigger(seconds=1))
        scheduler.wait_until_stopped()
except (KeyboardInterrupt, SystemExit):
    pass
