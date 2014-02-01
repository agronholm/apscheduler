"""
This example demonstrates the use of the MongoDB job store.
On each run, it adds a new alarm that fires after ten seconds.
You can exit the program, restart it and observe that any previous alarms that have not fired yet are still active.
Running the example with the --clear switch will remove any existing alarms.
"""

from datetime import datetime, timedelta
import sys

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore


def alarm(time):
    print('Alarm! This alarm was scheduled at %s.' % time)


if __name__ == '__main__':
    scheduler = BlockingScheduler()
    scheduler.add_jobstore(MongoDBJobStore(collection='example_jobs'))
    alarm_time = datetime.now() + timedelta(seconds=10)
    scheduler.add_job(alarm, 'date', [alarm_time], args=[datetime.now()])
    print('To clear the alarms, run this example with the --clear argument.')
    print('Press Ctrl+C to exit')

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
