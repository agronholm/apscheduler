"""
Demonstrates how to use the Twisted compatible scheduler to schedule a job that executes on 3 second intervals.
"""

from datetime import datetime

from twisted.internet import reactor
from apscheduler.schedulers.twisted import TwistedScheduler


def tick():
    print('Tick! The time is: %s' % datetime.now())


if __name__ == '__main__':
    scheduler = TwistedScheduler()
    scheduler.add_job('interval', tick, seconds=3)
    scheduler.start()
    print('Press Ctrl+C to exit')

    # Execution will block here until Ctrl+C is pressed.
    try:
        reactor.run()
    except (KeyboardInterrupt, SystemExit):
        pass
