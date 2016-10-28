from __future__ import absolute_import

import sys
from concurrent.futures import ThreadPoolExecutor

from tornado.gen import convert_yielded

from apscheduler.executors.base import BaseExecutor

try:
    from inspect import iscoroutinefunction
except ImportError:
    def iscoroutinefunction(func):
        return False


class TornadoExecutor(BaseExecutor):
    """
    Runs jobs either in a thread pool or directly on the I/O loop.

    If the job function is a native coroutine function, it is scheduled to be run directly in the
    I/O loop as soon as possible. All other functions are run in a thread pool.

    Plugin alias: ``tornado``

    :param int max_workers: maximum number of worker threads in the thread pool
    """

    def __init__(self, max_workers=10):
        super().__init__()
        self.executor = ThreadPoolExecutor(max_workers)

    def start(self, scheduler, alias):
        super(TornadoExecutor, self).start(scheduler, alias)
        self._ioloop = scheduler._ioloop

    def _do_submit_job(self, job, run_times, run_job_func):
        def callback(f):
            try:
                events = f.result()
            except:
                self._run_job_error(job.id, *sys.exc_info()[1:])
            else:
                self._run_job_success(job.id, events)

        # run_job_func is either a coroutine function, or a regular function 
        if iscoroutinefunction(job.func):
            f = run_job_func(job, job._jobstore_alias, run_times, self._logger.name)
        else:
            f = self.executor.submit(run_job_func, job, job._jobstore_alias, run_times,
                                     self._logger.name)

        f = convert_yielded(f)
        f.add_done_callback(callback)
