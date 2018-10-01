"""
This example demonstrates the use of the Peewee job store.
On each run, it adds a new alarm that fires after ten seconds.
You can exit the program, restart it and observe that any previous alarms that have not fired yet
are still active. You can also give it the database URL as an argument.
See the Peewee documentation on how to construct those.
"""

from datetime import datetime, timedelta
import sys
import os

from apscheduler.jobstores.peewee import Databases
from apscheduler.schedulers.blocking import BlockingScheduler


def alarm(time):
    print('Alarm! This alarm was scheduled at %s.' % time)


if __name__ == '__main__':
    scheduler = BlockingScheduler()
    database = sys.argv[1] if len(sys.argv) > 1 else 'example.sqlite'
    scheduler.add_jobstore('peewee', database=database, database_type=Databases.SQLITE)
    alarm_time = datetime.now() + timedelta(seconds=10)
    scheduler.add_job(alarm, 'date', run_date=alarm_time, args=[datetime.now()])
    print('To clear the alarms, delete the example.sqlite file.')
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
