from __future__ import absolute_import

import sys
from concurrent.futures import ThreadPoolExecutor

from tornado.gen import convert_yielded

from apscheduler.executors.base import BaseExecutor, run_job

try:
    from inspect import iscoroutinefunction
    from apscheduler.executors.base_py3 import run_coroutine_job
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

    def _do_submit_job(self, job, job_submission_id, run_time):
        def callback(f):
            event = f.result()
            self._handle_job_event(event)

        if iscoroutinefunction(job.func):
            f = run_coroutine_job(job, self._logger.name, job_submission_id, job._jobstore_alias, run_time)
        else:
            f = self.executor.submit(run_job, job, self._logger.name, job_submission_id, job._jobstore_alias, run_time)

        f = convert_yielded(f)
        f.add_done_callback(callback)
