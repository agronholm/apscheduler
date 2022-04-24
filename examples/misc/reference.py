"""
Basic example showing how to schedule a callable using a textual reference.
"""

from __future__ import annotations

import os

from apscheduler.schedulers.blocking import BlockingScheduler

if __name__ == "__main__":
    scheduler = BlockingScheduler()
    scheduler.add_job("sys:stdout.write", "interval", seconds=3, args=["tick\n"])
    print("Press Ctrl+{} to exit".format("Break" if os.name == "nt" else "C"))

    try:
        scheduler.initialize()
    except (KeyboardInterrupt, SystemExit):
        pass
