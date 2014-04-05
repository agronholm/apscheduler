from collections import defaultdict
from datetime import timedelta, datetime
from functools import partial
import concurrent.futures
import logging
import sys

from dateutil.tz import tzutc

from apscheduler.executors.base import BaseExecutor, MaxInstancesReachedError
from apscheduler.events import JobEvent, EVENT_JOB_MISSED, EVENT_JOB_ERROR, EVENT_JOB_EXECUTED

logger = logging.getLogger(__name__)
utc = tzutc()


def run_job(job, run_times):
    events = []
    for run_time in run_times:
        # See if the job missed its run time window, and handle possible misfires accordingly
        difference = datetime.now(utc) - run_time
        grace_time = timedelta(seconds=job.misfire_grace_time)
        if difference > grace_time:
            events.append(JobEvent(EVENT_JOB_MISSED, job, run_time))
            logger.warning('Run time of job "%s" was missed by %s', job, difference)
            continue

        logger.info('Running job "%s" (scheduled at %s)', job, run_time)
        try:
            retval = job.func(*job.args, **job.kwargs)
        except:
            exc, tb = sys.exc_info()[1:]
            events.append(JobEvent(EVENT_JOB_ERROR, job, run_time, exception=exc, traceback=tb))
            logger.exception('Job "%s" raised an exception', job)
        else:
            events.append(JobEvent(EVENT_JOB_EXECUTED, job, run_time, retval=retval))
            logger.info('Job "%s" executed successfully', job)

    return events


class BasePoolExecutor(BaseExecutor):
    def __init__(self, scheduler, executor, logger=None):
        super(BasePoolExecutor, self).__init__()
        self._instances = defaultdict(lambda: 0)
        self._scheduler = scheduler
        self._executor = executor
        self._logger = logger or logging.getLogger(__name__)
        self._lock = scheduler._create_lock()

    def submit_job(self, job, run_times):
        with self._lock:
            if self._instances[job.id] >= job.max_instances:
                raise MaxInstancesReachedError
            f = self._executor.submit(run_job, job, run_times)
            f.add_done_callback(partial(self.job_finished, job.id))
            self._instances[job.id] += 1

    def job_finished(self, job_id, future):
        with self._lock:
            self._instances[job_id] -= 1

        # Dispatch all the events that the worker produced while running the target function
        try:
            events = future.result()
        except Exception as e:
            logger.exception('Error executing job %s' % job_id)
        else:
            for event in events:
                self._scheduler._notify_listeners(event)

    def shutdown(self, wait=True):
        self._executor.shutdown(wait)


class ThreadPoolExecutor(BasePoolExecutor):
    def __init__(self, scheduler, max_workers=20):
        executor = concurrent.futures.ThreadPoolExecutor(max_workers)
        logger = logging.getLogger(__name__)
        super(ThreadPoolExecutor, self).__init__(scheduler, executor, logger)


class ProcessPoolExecutor(BasePoolExecutor):
    def __init__(self, scheduler, max_workers=5):
        executor = concurrent.futures.ProcessPoolExecutor(max_workers)
        logger = logging.getLogger(__name__)
        super(ProcessPoolExecutor, self).__init__(scheduler, executor, logger)
