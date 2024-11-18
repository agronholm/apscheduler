"""
Demonstrates how to schedule a job to be run in a process pool on 3 second intervals.
"""

import os
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler


def tick():
    print(f"Tick! The time is: {datetime.now()}")


if __name__ == "__main__":
    scheduler = BlockingScheduler()
    scheduler.add_executor("processpool")
    scheduler.add_job(tick, "interval", seconds=3)
    print("Press Ctrl+{} to exit".format("Break" if os.name == "nt" else "C"))

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
