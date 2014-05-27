"""
Demonstrates how to use the Twisted compatible scheduler to schedule a job that executes on 3 second intervals.
"""

from datetime import datetime
import os

from twisted.internet import reactor
from apscheduler.schedulers.twisted import TwistedScheduler


def tick():
    print('Tick! The time is: %s' % datetime.now())


if __name__ == '__main__':
    scheduler = TwistedScheduler()
    scheduler.add_job(tick, 'interval', seconds=3)
    scheduler.start()
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    # Execution will block here until Ctrl+C (Ctrl+Break on Windows) is pressed.
    try:
        reactor.run()
    except (KeyboardInterrupt, SystemExit):
        pass
