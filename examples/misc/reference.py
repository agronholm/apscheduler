"""
Basic example showing how to schedule a callable using a textual reference.
"""

import os

from apscheduler.schedulers.blocking import BlockingScheduler


if __name__ == '__main__':
    scheduler = BlockingScheduler()
    scheduler.add_job('sys:stdout.write', 'interval', seconds=3, args=['tick\n'])
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
