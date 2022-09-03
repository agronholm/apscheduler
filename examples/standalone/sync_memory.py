"""
Example demonstrating use of the synchronous scheduler.

To run: python sync_memory.py

It should print a line on the console on a one-second interval.
"""

from __future__ import annotations

from datetime import datetime

from apscheduler.schedulers.sync import Scheduler
from apscheduler.triggers.interval import IntervalTrigger


def tick():
    print("Hello, the time is", datetime.now())


with Scheduler() as scheduler:
    scheduler.add_schedule(tick, IntervalTrigger(seconds=1))
    scheduler.run_until_stopped()
