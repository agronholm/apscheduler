"""
Generic thread pool class. Modeled after Java's ThreadPoolExecutor.
Please note that this ThreadPool does *not* fully implement the PEP 3148
ThreadPool!
"""

from threading import Thread, Lock, currentThread
from weakref import ref
import logging
import atexit

try:
    from queue import Queue, Empty
except ImportError:
    from Queue import Queue, Empty

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
    def __init__(self, core_threads=0, max_threads=20, keepalive=1):
        """
        :param core_threads: maximum number of persistent threads in the pool
        :param max_threads: maximum number of total threads in the pool
        :param thread_class: callable that creates a Thread object
        :param keepalive: seconds to keep non-core worker threads waiting
            for new tasks
        """
        self.core_threads = core_threads
        self.max_threads = max(max_threads, core_threads, 1)
        self.keepalive = keepalive
        self._queue = Queue()
        self._threads_lock = Lock()
        self._threads = set()
        self._shutdown = False

        _threadpools.add(ref(self))
        logger.info('Started thread pool with %d core threads and %s maximum '
                    'threads', core_threads, max_threads or 'unlimited')

    def _adjust_threadcount(self):
        self._threads_lock.acquire()
        try:
            if self.num_threads < self.max_threads:
                self._add_thread(self.num_threads < self.core_threads)
        finally:
            self._threads_lock.release()

    def _add_thread(self, core):
        t = Thread(target=self._run_jobs, args=(core,))
        t.setDaemon(True)
        t.start()
        self._threads.add(t)

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

            try:
                func(*args, **kwargs)
            except:
                logger.exception('Error in worker thread')

        self._threads_lock.acquire()
        self._threads.remove(currentThread())
        self._threads_lock.release()

        logger.debug('Exiting worker thread')

    @property
    def num_threads(self):
        return len(self._threads)

    def submit(self, func, *args, **kwargs):
        if self._shutdown:
            raise RuntimeError('Cannot schedule new tasks after shutdown')

        self._queue.put((func, args, kwargs))
        self._adjust_threadcount()

    def shutdown(self, wait=True):
        if self._shutdown:
            return

        logging.info('Shutting down thread pool')
        self._shutdown = True
        _threadpools.remove(ref(self))

        self._threads_lock.acquire()
        for _ in range(self.num_threads):
            self._queue.put((None, None, None))
        self._threads_lock.release()

        if wait:
            self._threads_lock.acquire()
            threads = tuple(self._threads)
            self._threads_lock.release()
            for thread in threads:
                thread.join()

    def __repr__(self):
        if self.max_threads:
            threadcount = '%d/%d' % (self.num_threads, self.max_threads)
        else:
            threadcount = '%d' % self.num_threads

        return '<ThreadPool at %x; threads=%s>' % (id(self), threadcount)
