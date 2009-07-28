from datetime import datetime, timedelta
from time import sleep

from nose.tools import eq_, raises

from apscheduler.scheduler import Scheduler


class TestScheduler(object):
    def setUp(self):
        self.scheduler = Scheduler()
        self.scheduler.start()
        
    def tearDown(self):
        self.scheduler.shutdown()

    def test_configure(self):
        self.scheduler.configure({'daemonic_jobs': False, 'grace_seconds': 2})
    
    def test_delayed(self):
        vals = [0, 0]
        def increment(vals, amount):
            vals[0] += amount
            vals[1] += 1
        self.scheduler.add_delayed_job(increment, seconds=1, repeat=2, args=[vals, 2])
        sleep(3)
        eq_(vals[0], 4)
        eq_(vals[1], 2)
    
    def test_date(self):
        vals = []
        def append_val(value):
            vals.append(value)
        date = datetime.now() + timedelta(seconds=1)
        self.scheduler.add_job(append_val, date, kwargs={'value': 'test'})
        sleep(2)
        eq_(vals, ['test'])
