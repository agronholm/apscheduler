__all__ = ('EVENT_SCHEDULER_START', 'EVENT_SCHEDULER_SHUTDOWN',
           'EVENT_JOBSTORE_ADDED', 'EVENT_JOBSTORE_REMOVED',
           'EVENT_JOBSTORE_JOB_ADDED', 'EVENT_JOBSTORE_JOB_REMOVED',
           'EVENT_JOB_EXECUTED', 'EVENT_JOB_ERROR', 'EVENT_JOB_MISSED',
           'EVENT_ALL', 'SchedulerEvent', 'JobStoreEvent', 'JobEvent')


EVENT_SCHEDULER_START = 1        # The scheduler was started
EVENT_SCHEDULER_SHUTDOWN = 2     # The scheduler was shut down
EVENT_JOBSTORE_ADDED = 4         # A job store was added to the scheduler
EVENT_JOBSTORE_REMOVED = 8       # A job store was removed from the scheduler
EVENT_JOBSTORE_JOB_ADDED = 16    # A job was added to a job store
EVENT_JOBSTORE_JOB_REMOVED = 32  # A job was removed from a job store
EVENT_JOB_EXECUTED = 64          # A job was executed successfully
EVENT_JOB_ERROR = 128            # A job raised an exception during execution
EVENT_JOB_MISSED = 256           # A job's execution was missed
EVENT_ALL = (EVENT_SCHEDULER_START | EVENT_SCHEDULER_SHUTDOWN |
             EVENT_JOBSTORE_ADDED | EVENT_JOBSTORE_REMOVED |
             EVENT_JOBSTORE_JOB_ADDED | EVENT_JOBSTORE_JOB_REMOVED |
             EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | EVENT_JOB_MISSED)


class SchedulerEvent(object):
    """
    An event that concerns the scheduler itself.

    :var code: the type code of this event
    """
    def __init__(self, code):
        self.code = code


class JobStoreEvent(SchedulerEvent):
    """
    An event that concerns job stores.

    :var alias: the alias of the job store involved
    :var job: the new job if a job was added
    """
    def __init__(self, code, alias, job=None):
        SchedulerEvent.__init__(self, code)
        self.alias = alias
        if job:
            self.job = job


class JobEvent(SchedulerEvent):
    """
    An event that concerns the execution of individual jobs.

    :var job: the job instance in question
    :var scheduled_run_time: the time when the job was scheduled to be run
    :var retval: the return value of the successfully executed job
    :var exception: the exception raised by the job
    :var traceback: the traceback object associated with the exception
    """
    def __init__(self, code, job, scheduled_run_time, retval=None,
                 exception=None, traceback=None):
        SchedulerEvent.__init__(self, code)
        self.job = job
        self.scheduled_run_time = scheduled_run_time
        self.retval = retval
        self.exception = exception
        self.traceback = traceback
