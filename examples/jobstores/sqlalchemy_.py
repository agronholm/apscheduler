"""
This example demonstrates the use of the SQLAlchemy job store.
On each run, it adds a new alarm that fires after ten seconds.
You can exit the program, restart it and observe that any previous alarms that have not fired yet
are still active. You can also give it the database URL as an argument.
See the SQLAlchemy documentation on how to construct those.
"""

from datetime import datetime, timedelta
import sys
import os

from logging import StreamHandler
import logging

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler

l = logging.getLogger("apscheduler.scheduler")
l.addHandler(StreamHandler())
l.setLevel(logging.DEBUG)

l2= logging.getLogger("concurrent.futures")
l2.addHandler(StreamHandler())
l2.setLevel(logging.DEBUG)
l3 = logging.getLogger("apscheduler.executors.default")
l3.addHandler(StreamHandler())
l3.setLevel(logging.DEBUG)

def alarm(time):
    print('Alarm! This alarm was scheduled at %s.' % time)


if __name__ == '__main__':
    print "FUCK"
    scheduler = BlockingScheduler()
    url = sys.argv[1] if len(sys.argv) > 1 else 'sqlite:///example.sqlite'
    scheduler.add_jobstore('sqlalchemy', url=url)
    alarm_time = datetime.now() + timedelta(seconds=10)
    scheduler.add_job(alarm, 'date', run_date=alarm_time, args=[datetime.now()])
    print('To clear the alarms, delete the example.sqlite file.')
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print "TURD FERGUSON"
