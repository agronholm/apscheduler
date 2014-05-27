"""
Demonstrates how to use the Tornado compatible scheduler to schedule a job that executes on 3 second intervals.
"""

from datetime import datetime
import os

from tornado.ioloop import IOLoop
from apscheduler.schedulers.tornado import TornadoScheduler


def tick():
    print('Tick! The time is: %s' % datetime.now())


if __name__ == '__main__':
    scheduler = TornadoScheduler()
    scheduler.add_job(tick, 'interval', seconds=3)
    scheduler.start()
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    # Execution will block here until Ctrl+C (Ctrl+Break on Windows) is pressed.
    try:
        IOLoop.instance().start()
    except (KeyboardInterrupt, SystemExit):
        pass
