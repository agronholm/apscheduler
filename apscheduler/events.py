__all__ = ('EVENT_SCHEDULER_START', 'EVENT_SCHEDULER_SHUTDOWN', 'EVENT_EXECUTOR_ADDED',
           'EVENT_EXECUTOR_REMOVED', 'EVENT_JOBSTORE_ADDED', 'EVENT_JOBSTORE_REMOVED',
           'EVENT_ALL_JOBS_REMOVED', 'EVENT_JOB_ADDED', 'EVENT_JOB_REMOVED', 'EVENT_JOB_MODIFIED',
           'EVENT_JOB_EXECUTED', 'EVENT_JOB_ERROR', 'EVENT_JOB_MISSED', 'SchedulerEvent',
           'JobEvent', 'JobExecutionEvent')


EVENT_SCHEDULER_START = 1
EVENT_SCHEDULER_SHUTDOWN = 2
EVENT_EXECUTOR_ADDED = 4
EVENT_EXECUTOR_REMOVED = 8
EVENT_JOBSTORE_ADDED = 16
EVENT_JOBSTORE_REMOVED = 32
EVENT_ALL_JOBS_REMOVED = 64
EVENT_JOB_ADDED = 128
EVENT_JOB_REMOVED = 256
EVENT_JOB_MODIFIED = 512
EVENT_JOB_EXECUTED = 1024
EVENT_JOB_ERROR = 2048
EVENT_JOB_MISSED = 4096
EVENT_JOB_SUBMITTED = 8192
EVENT_JOB_MAX_INSTANCES = 16384
EVENT_ALL = (EVENT_SCHEDULER_START | EVENT_SCHEDULER_SHUTDOWN | EVENT_JOBSTORE_ADDED |
             EVENT_JOBSTORE_REMOVED | EVENT_JOB_ADDED | EVENT_JOB_REMOVED | EVENT_JOB_MODIFIED |
             EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | EVENT_JOB_MISSED | EVENT_JOB_SUBMITTED |
             EVENT_JOB_MAX_INSTANCES)


class SchedulerEvent(object):
    """
    An event that concerns the scheduler itself.

    :ivar code: the type code of this event
    :ivar alias: alias of the job store or executor that was added or removed (if applicable)
    """

    def __init__(self, code, alias=None):
        super(SchedulerEvent, self).__init__()
        self.code = code
        self.alias = alias

    def __repr__(self):
        return '<%s (code=%d)>' % (self.__class__.__name__, self.code)


class JobEvent(SchedulerEvent):
    """
    An event that concerns a job.

    :ivar code: the type code of this event
    :ivar job_id: identifier of the job in question
    :ivar jobstore: alias of the job store containing the job in question
    """

    def __init__(self, code, job_id, jobstore):
        super(JobEvent, self).__init__(code)
        self.code = code
        self.job_id = job_id
        self.jobstore = jobstore


class JobSubmissionEvent(JobEvent):
    """
    An event that concerns the submission of a job to its executor.

    :ivar scheduled_run_times: a list of datetimes when the job was intended to run
    """

    def __init__(self, code, job_id, jobstore, scheduled_run_times):
        super(JobSubmissionEvent, self).__init__(code, job_id, jobstore)
        self.scheduled_run_times = scheduled_run_times


class JobExecutionEvent(JobEvent):
    """
    An event that concerns the running of a job within its executor.

    :ivar scheduled_run_time: the time when the job was scheduled to be run
    :ivar retval: the return value of the successfully executed job
    :ivar exception: the exception raised by the job
    :ivar traceback: a formatted traceback for the exception
    """

    def __init__(self, code, job_id, jobstore, scheduled_run_time, retval=None, exception=None,
                 traceback=None):
        super(JobExecutionEvent, self).__init__(code, job_id, jobstore)
        self.scheduled_run_time = scheduled_run_time
        self.retval = retval
        self.exception = exception
        self.traceback = traceback
