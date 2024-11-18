"""
This example demonstrates the use of the etcd job store.
On each run, it adds a new alarm that fires after ten seconds.
You can exit the program, restart it and observe that any previous alarms that have not fired yet
are still active. Running the example with the --clear switch will remove any existing alarms.
"""

import os
import sys
from datetime import datetime, timedelta

from apscheduler.schedulers.blocking import BlockingScheduler


def alarm(time):
    print(f"Alarm! This alarm was scheduled at {time}.")


if __name__ == "__main__":
    scheduler = BlockingScheduler()
    scheduler.add_jobstore("etcd", alias="etcd", path="/example_jobs")
    if len(sys.argv) > 1 and sys.argv[1] == "--clear":
        scheduler.remove_all_jobs()

    alarm_time = datetime.now() + timedelta(seconds=10)
    scheduler.add_job(alarm, "date", run_date=alarm_time, args=[datetime.now()])
    print("To clear the alarms, run this example with the --clear argument.")
    print("Press Ctrl+{} to exit".format("Break" if os.name == "nt" else "C"))

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
