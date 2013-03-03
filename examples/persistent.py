"""
This example demonstrates the use of persistent job stores.
On each run, it adds a new alarm that fires after ten seconds.
You can exit the program, restart it and observe that any previous alarms that have not fired yet are still active.
"""

from datetime import datetime, timedelta

from apscheduler.scheduler import Scheduler
from apscheduler.jobstores.shelve_store import ShelveJobStore


def alarm(time):
    print('Alarm! This alarm was scheduled at %s.' % time)


if __name__ == '__main__':
    scheduler = Scheduler(standalone=True)
    scheduler.add_jobstore(ShelveJobStore('example.db'), 'shelve')
    alarm_time = datetime.now() + timedelta(seconds=10)
    scheduler.add_date_job(alarm, alarm_time, name='alarm',
                           jobstore='shelve', args=[datetime.now()])
    print('To clear the alarms, delete the example.db file.')
    print('Press Ctrl+C to exit')
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
