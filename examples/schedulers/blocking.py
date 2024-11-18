"""
Demonstrates how to use the blocking scheduler to schedule a job that executes on 3 second
intervals.
"""

import os
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler


def tick():
    print("Tick! The time is: %s" % datetime.now())


if __name__ == "__main__":
    scheduler = BlockingScheduler()
    scheduler.add_job(tick, "interval", seconds=3)
    print("Press Ctrl+{0} to exit".format("Break" if os.name == "nt" else "C"))

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
