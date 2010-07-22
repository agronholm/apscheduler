from Queue import Queue, Empty
from threading import Thread, Lock
import logging

try:
    from _thread import get_ident
except ImportError:
    from thread import get_ident



logger = logging.getLogger(__name__)

class ThreadPool(object):
    def __init__(self, core_threads=0, max_threads=None, thread_class=Thread):
        """
        :param core_threads: maximum number of persistent threads in the pool
        :param max_threads: maximum number of total threads in the pool
        :param thread_class: callable that creates a Thread object
        """
        self.queue = Queue()
        self.closed = False
        self.threads_lock = Lock()
        self.num_threads = 0
        self.busy_threads = 0
        self.core_threads = core_threads
        self.max_threads = max_threads
        self.thread_class = thread_class

        if max_threads:
            self.max_threads = max(max_threads, core_threads, 1)

        logger.info('Started thread pool with %d core threads and %s maximum '
                    'threads', core_threads, max_threads or 'unlimited')

    def _add_thread(self):
        core = self.num_threads < self.core_threads
        t = self.thread_class(target=self._run_jobs, args=(core,))
        t.setDaemon(True)
        t.start()

    def _add_threadcount(self, increment):
        self.threads_lock.acquire()
        self.num_threads += increment
        self.threads_lock.release()

    def _add_busycount(self, increment):
        self.threads_lock.acquire()
        self.busy_threads += increment
        self.threads_lock.release()

    def _run_jobs(self, core):
        logger.debug('Started thread (id=%d)', get_ident())
        self._add_threadcount(1)

        while not self.closed:
            try:
                if core:
                    func, args, kwargs = self.queue.get()
                else:
                    func, args, kwargs = self.queue.get_nowait()
            except Empty:
                break

            self._add_busycount(1)
            try:
                try:
                    func(*args, **kwargs)
                except:
                    pass
            finally:
                self._add_busycount(-1)
                self.queue.task_done()

        self._add_threadcount(-1)
        logger.debug('Exiting thread (id=%d)', get_ident())

    def execute(self, func, args=(), kwargs={}):
        self.threads_lock.acquire()
        try:
            if (self.busy_threads == self.num_threads and not
                self.max_threads or self.num_threads < self.max_threads):
                self._add_thread()
            self.queue.put((func, args, kwargs))
        finally:
            self.threads_lock.release()

    def close(self, timeout):
        logger.info('Shutting down thread pool')
        self.closed = True
        for t in self.threads:
            t.join(timeout)
