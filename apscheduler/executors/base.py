from abc import ABCMeta, abstractmethod
from collections import defaultdict
from datetime import datetime, timedelta
from traceback import format_tb
import logging
import sys

from pytz import utc
import six

from apscheduler.events import (
    JobExecutionEvent, EVENT_JOB_MISSED, EVENT_JOB_ERROR, EVENT_JOB_EXECUTED)


class MaxInstancesReachedError(Exception):
    def __init__(self, job):
        super(MaxInstancesReachedError, self).__init__(
            'Job "%s" has already reached its maximum number of instances (%d)' %
            (job.id, job.max_instances))


class BaseExecutor(six.with_metaclass(ABCMeta, object)):
    """Abstract base class that defines the interface that every executor must implement."""

    _scheduler = None
    _lock = None
    _logger = logging.getLogger('apscheduler.executors')

    def __init__(self):
        super(BaseExecutor, self).__init__()
        self._instances = defaultdict(lambda: 0)

    def start(self, scheduler, alias):
        """
        Called by the scheduler when the scheduler is being started or when the executor is being
        added to an already running scheduler.

        :param apscheduler.schedulers.base.BaseScheduler scheduler: the scheduler that is starting
            this executor
        :param str|unicode alias: alias of this executor as it was assigned to the scheduler

        """
        self._scheduler = scheduler
        self._lock = scheduler._create_lock()
        self._logger = logging.getLogger('apscheduler.executors.%s' % alias)

    def shutdown(self, wait=True):
        """
        Shuts down this executor.

        :param bool wait: ``True`` to wait until all submitted jobs
            have been executed
        """

    def submit_job(self, job, run_time):
        """
        Submits job for execution.

        :param Job job: job to execute
        :raises MaxInstancesReachedError: if the maximum number of
            allowed instances for this job has been reached

        """
        assert self._lock is not None, 'This executor has not been started yet'
        with self._lock:
            if self._instances[job.id] >= job.max_instances:
                raise MaxInstancesReachedError(job)

            job_submission_id = self._scheduler._add_job_submission(job)

            self._do_submit_job(job, job_submission_id, run_time)
            self._instances[job.id] += 1

    @abstractmethod
    def _do_submit_job(self, job, job_submission_id, run_time):
        """Performs the actual task of scheduling `run_job` to be called."""

    def _run_job_success(self, job_id, events):
        """
        Called by the executor with the list of generated events when :func:`run_job` has been
        successfully called.

        """
        with self._lock:
            self._instances[job_id] -= 1
            if self._instances[job_id] == 0:
                del self._instances[job_id]

        for event in events:
            self._scheduler._dispatch_event(event)

    def _run_job_error(self, job_id, exc, traceback=None):
        """Called by the executor with the exception if there is an error  calling `run_job`."""
        with self._lock:
            self._instances[job_id] -= 1
            if self._instances[job_id] == 0:
                del self._instances[job_id]

        exc_info = (exc.__class__, exc, traceback)
        self._logger.error('Error running job %s', job_id, exc_info=exc_info)


def run_job(job, logger_name, job_submission_id, run_time):
    """
    Called by executors to run the job. Returns a list of scheduler events to be dispatched by the
    scheduler.

    """
    events = []
    logger = logging.getLogger(logger_name)
    
    job._scheduler._update_job_submission(job_submission_id, job._jobstore_alias, state='running')
    logger.info('Running job "%s" (scheduled at %s)', job, run_time)
    try:
        retval = job.func(*job.args, **job.kwargs)
    except:
        exc, tb = sys.exc_info()[1:]
        formatted_tb = ''.join(format_tb(tb))
        events.append(JobExecutionEvent(EVENT_JOB_ERROR, job.id, job_submission_id, job.jobstore_alias, run_time,
                                        exception=exc, traceback=formatted_tb))
        job._scheduler._update_job_submission(job_submission_id, job._jobstore_alias, state='failure')
        logger.exception('Job "%s" raised an exception', job)
    else:
        events.append(JobExecutionEvent(EVENT_JOB_EXECUTED, job.id, job_submission_id, job.jobstore_alias, run_time,
                                        retval=retval))
        job._scheduler._update_job_submission(job_submission_id, job._jobstore_alias, state='success')
        logger.info('Job "%s" executed successfully', job)

    return events
