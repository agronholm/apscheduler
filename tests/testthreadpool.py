from threading import Event
from time import sleep

from nose.tools import eq_, assert_raises  # @UnresolvedImport

from apscheduler.threadpool import ThreadPool


def test_threadpool():
    pool = ThreadPool(core_threads=2, keepalive=0)
    event1 = Event()
    event2 = Event()
    event3 = Event()
    pool.submit(event1.set)
    pool.submit(event2.set)
    pool.submit(event3.set)
    event1.wait(1)
    event2.wait(1)
    event3.wait(1)
    assert event1.isSet()
    assert event2.isSet()
    assert event3.isSet()
    sleep(0.3)
    eq_(repr(pool), '<ThreadPool at %x; threads=2/20>' % id(pool))

    pool.shutdown()
    eq_(repr(pool), '<ThreadPool at %x; threads=0/20>' % id(pool))

    # Make sure double shutdown is ok
    pool.shutdown()

    # Make sure one can't submit tasks to a thread pool that has been shut down
    assert_raises(RuntimeError, pool.submit, event1.set)


def test_threadpool_maxthreads():
    pool = ThreadPool(core_threads=2, max_threads=1)
    eq_(pool.max_threads, 2)

    pool = ThreadPool(core_threads=2, max_threads=3)
    eq_(pool.max_threads, 3)

    pool = ThreadPool(core_threads=0, max_threads=0)
    eq_(pool.max_threads, 1)


def test_threadpool_nocore():
    pool = ThreadPool(keepalive=0)
    event = Event()
    pool.submit(event.set)
    event.wait(1)
    assert event.isSet()
    sleep(1)
    eq_(repr(pool), '<ThreadPool at %x; threads=0/20>' % id(pool))
