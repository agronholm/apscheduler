"""
Basic example showing how to schedule a callable using a textual reference.
"""

from apscheduler.scheduler import Scheduler


if __name__ == '__main__':
    scheduler = Scheduler(standalone=True)
    scheduler.add_job('sys:stdout.write', 'interval', {'seconds': 3}, args=['tick\n'])
    print('Press Ctrl+C to exit')
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
