from abc import ABCMeta, abstractmethod
import logging

import six


class JobLookupError(KeyError):
    """Raised when the job store cannot find a job for update or removal."""

    def __init__(self, job_id):
        super(JobLookupError, self).__init__(u'No job by the id of %s was found' % job_id)


class JobSubmissionLookupError(KeyError):
    """Raised when the job store cannot find the *job instance* of job for update or removal."""

    def __init__(self, job_id):
        super(JobSubmissionLookupError, self).\
            __init__(u'No job by the id of %s was found' % job_id)


class ConflictingIdError(KeyError):
    """Raised when the uniqueness of job IDs is being violated."""

    def __init__(self, job_id):
        super(ConflictingIdError, self).__init__(
            u'Job identifier (%s) conflicts with an existing job' % job_id)


class TransientJobError(ValueError):
    """
    Raised when an attempt to add transient (with no func_ref) job to a persistent job store is
    detected.
    """

    def __init__(self, job_id):
        super(TransientJobError, self).__init__(
            u'Job (%s) cannot be added to this job store because a reference to the callable '
            u'could not be determined.' % job_id)


class BaseJobStore(six.with_metaclass(ABCMeta)):
    """Abstract base class that defines the interface that every job store must implement."""

    _scheduler = None
    _alias = None
    _logger = logging.getLogger('apscheduler.jobstores')

    def start(self, scheduler, alias):
        """
        Called by the scheduler when the scheduler is being started or when the job store is being
        added to an already running scheduler.

        :param apscheduler.schedulers.base.BaseScheduler scheduler: the scheduler that is starting
            this job store
        :param str|unicode alias: alias of this job store as it was assigned to the scheduler
        """

        self._scheduler = scheduler
        self._alias = alias
        self._logger = logging.getLogger('apscheduler.jobstores.%s' % alias)

    def update_orphans(self):
        # If jobstore didn't 'stop' gracefully,
        # set all non-finished job_submissions to "orphaned"
        self.update_job_submissions({"state": "submitted"}, state='orphaned')

    def shutdown(self):
        """
        Frees any resources still bound to this job store.
        """
        # Any jobs that haven't finished will be orphaned when we shutdown !
        self.update_job_submissions({"state": "submitted"}, state='orphaned')
        # TODO: Try and gracefully stop these jobs when we shutdown the jobstore...

    def _fix_paused_jobs_sorting(self, jobs):
        for i, job in enumerate(jobs):
            if job.next_run_time is not None:
                if i > 0:
                    paused_jobs = jobs[:i]
                    del jobs[:i]
                    jobs.extend(paused_jobs)
                break

    @abstractmethod
    def add_job_submission(self, job, now):
        """
        Adds a job submission to the jobstore, and returns the ID of the inserted record

        :param Job job: The job which has been submitted to be executed
        :param datetime now: The current datetime.now(timezone) value
        :rtype: int
        """

    @abstractmethod
    def update_job_submissions(self, conditions, **kwargs):
        """
        Finds all job submissions which satisfy ``conditions``, and updates them according to the
        columns/values in ``kwargs``.

        :param dict conditions: A dict of columns => values which is used to build a condition
        to choose which rows to update. ({'column1': 'value1', 'columnn2': 'value2'...}
        :param dict kwargs: A dict of columns => values which designates what columns to update
        in the job_submission store.

        """

    @abstractmethod
    def update_job_submission(self, job_submission_id, **kwargs):
        """
        Updates the job submission designated by ``job_submission_id``, specifically updating the
        record's attributes specified by keywords in **kwargs

        :param int job_submission_id: The ID of the job_submission in the job_store
        :param dict kwargs: A dictionary whos keys correspond to attributes (columns) of the
        apscheduler_job_submission collection in the jobstore. Common keywords (columns) passed
        to this function include: ``state``, ``started_at``, and ``completed_at``.

        """
    @abstractmethod
    def get_job_submissions_with_states(self, states=[]):
        """
        Returns all job submissions in the jobstore which have a state in ``states``.

        :param list[str] states: List of strings in the set ['submitted','running','failure',
        'success','orphaned']. Function will fetch job_submissions with any of the provided
        states.
        :rtype: list[dict]
        """
    @abstractmethod
    def get_job_submission(self, job_submission_id):
        """
        Returns the specific job submission with ID ``job_submission_id``.

        :param int job_submission_id: The ID of the ``job_submission` to fetch.
        :rtype: dict
        """

    @abstractmethod
    def lookup_job(self, job_id):
        """
        Returns a specific job, or ``None`` if it isn't found..

        The job store is responsible for setting the ``scheduler`` and ``jobstore`` attributes of
        the returned job to point to the scheduler and itself, respectively.

        :param str|unicode job_id: identifier of the job
        :rtype: Job
        """

    @abstractmethod
    def get_due_jobs(self, now):
        """
        Returns the list of jobs that have ``next_run_time`` earlier or equal to ``now``.
        The returned jobs must be sorted by next run time (ascending).

        :param datetime.datetime now: the current (timezone aware) datetime
        :rtype: list[Job]
        """

    @abstractmethod
    def get_next_run_time(self):
        """
        Returns the earliest run time of all the jobs stored in this job store, or ``None`` if
        there are no active jobs.

        :rtype: datetime.datetime
        """

    @abstractmethod
    def get_all_jobs(self):
        """
        Returns a list of all jobs in this job store.
        The returned jobs should be sorted by next run time (ascending).
        Paused jobs (next_run_time == None) should be sorted last.

        The job store is responsible for setting the ``scheduler`` and ``jobstore`` attributes of
        the returned jobs to point to the scheduler and itself, respectively.

        :rtype: list[Job]
        """

    @abstractmethod
    def add_job(self, job):
        """
        Adds the given job to this store.

        :param Job job: the job to add
        :raises ConflictingIdError: if there is another job in this store with the same ID
        """

    @abstractmethod
    def update_job(self, job):
        """
        Replaces the job in the store with the given newer version.

        :param Job job: the job to update
        :raises JobLookupError: if the job does not exist
        """

    @abstractmethod
    def remove_job(self, job_id):
        """
        Removes the given job from this store.

        :param str|unicode job_id: identifier of the job
        :raises JobLookupError: if the job does not exist
        """

    @abstractmethod
    def remove_all_jobs(self):
        """Removes all jobs from this store."""

    def __repr__(self):
        return '<%s>' % self.__class__.__name__
