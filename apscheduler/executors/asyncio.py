from __future__ import absolute_import

import asyncio
import sys
from traceback import format_tb

from apscheduler.events import JobExecutionEvent, EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from apscheduler.executors.base import BaseExecutor


class AsyncIOExecutor(BaseExecutor):
    """
    Runs jobs in the default executor of the event loop.

    Plugin alias: ``asyncio``
    """

    def start(self, scheduler, alias):
        super(AsyncIOExecutor, self).start(scheduler, alias)
        self._eventloop = scheduler._eventloop

    def _do_submit_job(self, job, run_times):
        asyncio.get_event_loop().call_soon(self._do_job_runtime, job, run_times)

    def _do_job_runtime(self, job, run_times):
        def callback(f):
            try:
                events = f.result()
            except:
                self._run_job_error(job.id, *sys.exc_info()[1:])
            else:
                self._run_job_success(job.id, events)

        events = self._job_runtime(job, run_times)
        future_events = []
        for event in events:
            if not (isinstance(event, asyncio.Future) or asyncio.iscoroutine(event)):
                future = asyncio.Future()
                future.set_result(event)
                future_events.append(future)
            else:
                future_events.append(event)
        future = asyncio.gather(*events)
        future.add_done_callback(callback)

    @asyncio.coroutine
    def _run_job(self, job, run_time):
        """Actual implementation of calling the job function"""
        try:
            retval = yield from job.func(*job.args, **job.kwargs)
        except:
            exc, tb = sys.exc_info()[1:]
            formatted_tb = ''.join(format_tb(tb))
            self._logger.exception('Job "%s" raised an exception', job)
            return JobExecutionEvent(EVENT_JOB_ERROR, job.id, job._jobstore_alias, run_time,
                                     exception=exc, traceback=formatted_tb)
        else:
            self._logger.info('Job "%s" executed successfully', job)
            return JobExecutionEvent(EVENT_JOB_EXECUTED, job.id, job._jobstore_alias, run_time, retval=retval)
