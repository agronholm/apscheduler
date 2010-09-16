"""
Generic thread pool class. Modeled after Java's ThreadPoolExecutor.
"""

from Queue import Queue, Empty
from threading import Thread, Lock, currentThread
from weakref import ref
import logging
import atexit


logger = logging.getLogger(__name__)
_threadpools = set()

# Worker threads are daemonic in order to let the interpreter exit without
# an explicit shutdown of the thread pool. The following trick is necessary
# to allow worker threads to finish cleanly.
def _shutdown_all():
    for pool_ref in tuple(_threadpools):
        pool = pool_ref()
        if pool:
            pool.shutdown()

atexit.register(_shutdown_all)


class ThreadPool(object):
    def __init__(self, core_threads=0, max_threads=None, keepalive=1):
        """
        :param core_threads: maximum number of persistent threads in the pool
        :param max_threads: maximum number of total threads in the pool
        :param thread_class: callable that creates a Thread object
        :param keepalive: seconds to keep non-core worker threads waiting
            for new tasks
        """
        self.core_threads = core_threads
        self.max_threads = max_threads
        self.keepalive = keepalive
        self._queue = Queue()
        self._threads_lock = Lock()
        self._threads = set()
        self._busy_threads = 0
        self._shutdown = False

        if max_threads is not None:
            self.max_threads = max(max_threads, core_threads, 1)

        _threadpools.add(ref(self))
        logger.info('Started thread pool with %d core threads and %s maximum '
                    'threads', core_threads, max_threads or 'unlimited')

    def _add_thread(self):
        core = self.num_threads < self.core_threads
        t = Thread(target=self._run_jobs, args=(core,))
        t.setDaemon(True)
        t.start()
        self._threads.add(t)

    def _add_busycount(self, increment):
        self._threads_lock.acquire()
        self._busy_threads += increment
        self._threads_lock.release()

    def _run_jobs(self, core):
        logger.debug('Started worker thread')
        block = True
        timeout = None
        if not core:
            block = self.keepalive > 0
            timeout = self.keepalive

        while True:
            try:
                func, args, kwargs = self._queue.get(block, timeout)
            except Empty:
                break

            if self._shutdown:
                break

            self._add_busycount(1)
            try:
                func(*args, **kwargs)
            except:
                logger.exception('Error in worker thread')
            self._add_busycount(-1)

        self._threads_lock.acquire()
        self._threads.discard(currentThread())
        self._threads_lock.release()

        logger.debug('Exiting worker thread')

    @property
    def num_threads(self):
        return len(self._threads)

    @property
    def busy_threads(self):
        return self._busy_threads

    def execute(self, func, args=(), kwargs={}):
        if self._shutdown:
            raise Exception('Thread pool has already been shut down')

        self._threads_lock.acquire()
        try:
            self._queue.put((func, args, kwargs))
            if (self.busy_threads == self.num_threads and not
                self.max_threads or self.num_threads < self.max_threads):
                self._add_thread()
        finally:
            self._threads_lock.release()

    def shutdown(self, wait=True):
        if self._shutdown:
            return

        self._shutdown = True
        logging.info('Shutting down thread pool')

        self._threads_lock.acquire()
        for _ in range(len(self._threads)):
            self._queue.put((None, None, None))
        self._threads_lock.release()

        if wait:
            for thread in tuple(self._threads):
                thread.join()

    def __repr__(self):
        return '<ThreadPool at %x; threads=%d busy=%d>' % (id(self),
            len(self._threads), self._busy_threads)
