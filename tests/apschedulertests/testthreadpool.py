from threading import Event
from time import sleep

from nose.tools import eq_

from apscheduler.threadpool import ThreadPool


def test_threadpool():
    pool = ThreadPool(keepalive=0)
    event = Event()
    pool.execute(event.set)
    event.wait(1)
    assert event.isSet()
    sleep(1)
    eq_(pool.busy_threads, 0)
    eq_(pool.num_threads, 0)


def test_threadpool_corethreads():
    pool = ThreadPool(core_threads=1, keepalive=0)
    event1 = Event()
    event2 = Event()
    pool.execute(event1.set)
    pool.execute(event2.set)
    event1.wait(1)
    event2.wait(1)
    assert event1.isSet()
    assert event2.isSet()
    sleep(1)
    eq_(pool.busy_threads, 0)
    eq_(pool.num_threads, 1)
