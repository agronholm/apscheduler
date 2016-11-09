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

    def _do_submit_job(self, job, job_submission_id, run_time):
        def callback(f):
            event = f.result()
            self._handle_job_event(event)

        if iscoroutinefunction(job.func):
            if run_coroutine_job is not None:
                coro = run_coroutine_job(job, self._logger.name, job_submission_id, job._jobstore_alias, run_time)
                f = self._eventloop.create_task(coro)
            else:
                raise Exception('Executing coroutine based jobs is not supported with Trollius')
        else:
            f = self._eventloop.run_in_executor(None, run_job, job, self._logger.name,
                                               job_submission_id, job._jobstore_alias, run_time)

        f.add_done_callback(callback)
