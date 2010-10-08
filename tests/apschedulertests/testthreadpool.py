from threading import Event
from time import sleep

from nose.tools import eq_, assert_raises

from apscheduler.threadpool import ThreadPool


def test_threadpool():
    pool = ThreadPool(core_threads=2, keepalive=0)
    event1 = Event()
    event2 = Event()
    event3 = Event()
    pool.execute(event1.set)
    pool.execute(event2.set)
    pool.execute(event3.set)
    event1.wait(1)
    event2.wait(1)
    event3.wait(1)
    assert event1.isSet()
    assert event2.isSet()
    assert event3.isSet()
    eq_(repr(pool), '<ThreadPool at %x; threads=2>' % id(pool))

    pool.shutdown()
    eq_(repr(pool), '<ThreadPool at %x; threads=0>' % id(pool))

    # Make sure double shutdown is ok
    pool.shutdown()

    # Make sure one can't submit tasks to a thread pool that has been shut down
    assert_raises(Exception, pool.execute, event1.set)


def test_threadpool_nocore():
    pool = ThreadPool(keepalive=0)
    event = Event()
    pool.execute(event.set)
    event.wait(1)
    assert event.isSet()
    sleep(1)
    eq_(repr(pool), '<ThreadPool at %x; threads=0>' % id(pool))
