from __future__ import absolute_import

import sys

from apscheduler.executors.base import BaseExecutor, run_job

try:
    from asyncio import iscoroutinefunction
    from apscheduler.executors.base_py3 import run_coroutine_job
except ImportError:
    from trollius import iscoroutinefunction
    run_coroutine_job = None


class AsyncIOExecutor(BaseExecutor):
    """
    Runs jobs in the default executor of the event loop.

    If the job function is a native coroutine function, it is scheduled to be run directly in the
    event loop as soon as possible. All other functions are run in the event loop's default
    executor which is usually a thread pool.

    Plugin alias: ``asyncio``
    """

    def start(self, scheduler, alias):
        super(AsyncIOExecutor, self).start(scheduler, alias)
        self._eventloop = scheduler._eventloop

    def _do_submit_job(self, job, run_times):
        def callback(f):
            try:
                events = f.result()
            except:
                self._run_job_error(job.id, *sys.exc_info()[1:])
            else:
                self._run_job_success(job.id, events)

        if iscoroutinefunction(job.func):
            if run_coroutine_job is not None:
                coro = run_coroutine_job(job, job._jobstore_alias, run_times, self._logger.name)
                f = self._eventloop.create_task(coro)
            else:
                raise Exception('Executing coroutine based jobs is not supported with Trollius')
        else:
            f = self._eventloop.run_in_executor(None, run_job, job, job._jobstore_alias, run_times,
                                                self._logger.name)

        f.add_done_callback(callback)
