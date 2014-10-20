from __future__ import absolute_import
import sys

from apscheduler.executors.base import BaseExecutor, run_job


class AsyncIOExecutor(BaseExecutor):
    """
    Runs jobs in the default executor of the event loop.

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

        f = self._eventloop.run_in_executor(None, run_job, job, job._jobstore_alias, run_times, self._logger.name)
        f.add_done_callback(callback)
