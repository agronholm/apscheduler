"""
Basic example showing how to schedule a callable using a textual reference.
"""

from apscheduler.scheduler import Scheduler


if __name__ == '__main__':
    scheduler = Scheduler(standalone=True)
    scheduler.add_interval_job('sys:stdout.write', args=['tick\n'], seconds=3)
    print('Press Ctrl+C to exit')
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
