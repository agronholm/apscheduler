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
    def __init__(self, code):
        self.code = code


class JobStoreEvent(SchedulerEvent):
    def __init__(self, code, alias, job=None):
        SchedulerEvent.__init__(self, code)
        self.alias = alias
        if job:
            self.job = job


class JobEvent(SchedulerEvent):
    def __init__(self, code, job, scheduled_run_time, retval=None,
                 exception=None, traceback=None):
        SchedulerEvent.__init__(self, code)
        self.job = job
        self.scheduled_run_time = scheduled_run_time
        self.retval = retval
        self.exception = exception
        self.traceback = traceback
