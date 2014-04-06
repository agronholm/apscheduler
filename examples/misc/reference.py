"""
Basic example showing how to schedule a callable using a textual reference.
"""

from apscheduler.schedulers.blocking import BlockingScheduler


if __name__ == '__main__':
    scheduler = BlockingScheduler()
    scheduler.add_job('sys:stdout.write', 'interval', seconds=3, args=['tick\n'])
    print('Press Ctrl+C to exit')

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
