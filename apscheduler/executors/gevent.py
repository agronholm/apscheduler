from __future__ import absolute_import
import sys

from apscheduler.executors.base import BaseExecutor, run_job


try:
    import gevent
except ImportError:  # pragma: nocover
    raise ImportError('GeventExecutor requires gevent installed')


class GeventExecutor(BaseExecutor):
    """
    Runs jobs as greenlets.

    Plugin alias: ``gevent``
    """

    def _do_submit_job(self, job, job_submission_id, run_time):
        def callback(greenlet):
            try:
                events = greenlet.get()
            except:
                self._run_job_error(job.id, *sys.exc_info()[1:])
            else:
                self._run_job_success(job.id, events)

        gevent.spawn(run_job, job, self._logger.name, job_submission_id, run_time).\
            link(callback)
