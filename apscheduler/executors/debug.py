import sys

from apscheduler.executors.base import BaseExecutor, run_job


class DebugExecutor(BaseExecutor):
    """
    A special executor that executes the target callable directly instead of deferring it to a
    thread or process.

    Plugin alias: ``debug``
    """

    def _do_submit_job(self, job, job_submission_id, run_time):
        try:
            events = run_job(job, self._logger.name, job_submission_id, job._jobstore_alias,
                             run_time)
        except:
            self._run_job_error(job.id, job_submission_id, job._jobstore_alias,
                                *sys.exc_info()[1:])
        else:
            self._run_job_success(job.id, job_submission_id, job._jobstore_alias, events)
