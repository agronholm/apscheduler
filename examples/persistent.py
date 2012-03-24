"""
This example demonstrates the use of persistent job stores. On each run, it
adds a new alarm that fires after one minute. You can exit the program, restart
it and observe that any previous alarms that have not fired yet are still
active.
"""

from datetime import datetime, timedelta
import sys
import time

from apscheduler.scheduler import Scheduler
from apscheduler.jobstores.shelve_store import ShelveJobStore


def alarm(time):
    sys.stdout.write('Alarm! This alarm was scheduled at %s.\n' % time)


if __name__ == '__main__':
    scheduler = Scheduler()
    scheduler.add_jobstore(ShelveJobStore('example.db'), 'shelve')
    alarm_time = datetime.now() + timedelta(minutes=1)
    scheduler.add_date_job(alarm, alarm_time, name='alarm',
                           jobstore='shelve', args=[datetime.now()])
    sys.stdout.write('To clear the alarms, delete the example.db file.\n')
    sys.stdout.write('Press Ctrl+C to exit\n')
    scheduler.start()

    try:
        # This is here to prevent the main thread from exiting so that the
        # scheduler has time to work -- this is rarely necessary in real world
        # applications
        time.sleep(9999)
    finally:
        # Shut down the scheduler so that the job store gets closed properly
        scheduler.shutdown()
