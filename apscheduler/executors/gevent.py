from __future__ import absolute_import

import sys

from apscheduler.executors.base import BaseExecutor, job_runtime

try:
    import gevent
except ImportError:  # pragma: nocover
    raise ImportError('GeventExecutor requires gevent installed')


class GeventExecutor(BaseExecutor):
    """
    Runs jobs as greenlets.

    Plugin alias: ``gevent``
    """

    def _do_submit_job(self, job, run_times):
        def callback(greenlet):
            try:
                events = greenlet.get()
            except:
                self._run_job_error(job.id, *sys.exc_info()[1:])
            else:
                self._run_job_success(job.id, events)

        gevent.spawn(job_runtime, job, run_times, self._logger.name, job._jobstore_alias) \
            .link(callback)
