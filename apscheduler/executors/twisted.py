from __future__ import absolute_import

from apscheduler.executors.base import BaseExecutor, run_job


class TwistedExecutor(BaseExecutor):
    """
    Runs jobs in the reactor's thread pool.

    Plugin alias: ``twisted``
    """

    def start(self, scheduler, alias):
        super(TwistedExecutor, self).start(scheduler, alias)
        self._reactor = scheduler._reactor

    def _do_submit_job(self, job, job_submission_id, run_time):
        def callback(success, result):
            self._handle_job_event(result)

        self._reactor.getThreadPool().callInThreadWithCallback(
            callback, run_job, job, self._logger.name, job_submission_id, job._jobstore_alias, run_time)
