import logging
import sys
from datetime import datetime, timedelta
from traceback import format_tb

from pytz import utc

from apscheduler.events import (
    JobExecutionEvent, EVENT_JOB_MISSED, EVENT_JOB_ERROR, EVENT_JOB_EXECUTED)


async def run_coroutine_job(job, logger_name, job_submission_id, jobstore_alias, run_time):
    """Coroutine version of run_job()."""
    """
    Called by executors to run the job. Returns a list of scheduler events to be dispatched by the
    scheduler.

    """
    events = []
    logger = logging.getLogger(logger_name)
    
    #job._scheduler._update_job_submission(job_submission_id, job._jobstore_alias, state='running')
    logger.info('Running job "%s" (scheduled at %s)', job, run_time)
    try:
        retval = job.func(*job.args, **job.kwargs)
    except:
        exc, tb = sys.exc_info()[1:]
        formatted_tb = ''.join(format_tb(tb))
        events.append(JobExecutionEvent(EVENT_JOB_ERROR, job.id, jobstore_alias, run_time,
                                        exception=exc, traceback=formatted_tb))
        logger.exception('Job "%s" raised an exception', job)
    else:
        events.append(JobExecutionEvent(EVENT_JOB_EXECUTED, job.id, jobstore_alias, run_time,
                                        retval=retval))
        logger.info('Job "%s" executed successfully', job)

    return events
