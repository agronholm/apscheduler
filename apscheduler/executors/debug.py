import sys

from apscheduler.executors.base import BaseExecutor, job_runtime


class DebugExecutor(BaseExecutor):
    """
    A special executor that executes the target callable directly instead of deferring it to a
    thread or process.

    Plugin alias: ``debug``
    """

    def _do_submit_job(self, job, run_times):
        try:
            events = job_runtime(job, run_times, self._logger.name, job._jobstore_alias)
        except:
            self._run_job_error(job.id, *sys.exc_info()[1:])
        else:
            self._run_job_success(job.id, events)
