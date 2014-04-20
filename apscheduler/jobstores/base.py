"""
Abstract base class that provides the interface needed by all job stores.
Job store methods are also documented here.
"""

from abc import ABCMeta, abstractmethod
import logging

import six


class JobLookupError(KeyError):
    """Raised when the job store cannot find a job for update or removal."""

    def __init__(self, job_id):
        super(JobLookupError, self).__init__(six.u('No job by the id of %s was found') % job_id)


class ConflictingIdError(KeyError):
    """Raised when the uniqueness of job IDs is being violated."""

    def __init__(self, job_id):
        super(ConflictingIdError, self).__init__(six.u('Job identifier (%s) conflicts with an existing job') % job_id)


class TransientJobError(ValueError):
    """Raised when an attempt to add transient (with no func_ref) job to a persistent job store is detected."""

    def __init__(self, job_id):
        super(TransientJobError, self).__init__(
            six.u('Job (%s) cannot be added to this job store because a reference to the callable could not be '
                  'determined.') % job_id)


class BaseJobStore(six.with_metaclass(ABCMeta)):
    _logger = logging.getLogger('apscheduler.jobstores')

    def start(self, scheduler, alias):
        """
        Called by the scheduler when the scheduler is being started or when the job store is being added to an already
        running scheduler.

        :param apscheduler.schedulers.base.BaseScheduler scheduler: the scheduler that is starting this job store
        :param str|unicode alias: alias of this job store as it was assigned to the scheduler
        """

        self._logger = logging.getLogger('apscheduler.jobstores.%s' % alias)

    def shutdown(self):
        """Frees any resources still bound to this job store."""

    @abstractmethod
    def lookup_job(self, job_id):
        """
        Returns a specific job.

        :param str|unicode job_id: identifier of the job
        :rtype: Job
        :raises JobLookupError: if the job is not found
        """

    @abstractmethod
    def get_pending_jobs(self, now):
        """
        Returns the list of jobs that have ``next_run_time`` earlier or equal to ``now``, sorted by next run time

        :param datetime.datetime now: the current (timezone aware) datetime
        :rtype: list[Job]
        """

    @abstractmethod
    def get_next_run_time(self):
        """
        Returns the earliest run time of all the jobs stored in this job store, or ``None`` if there are no active jobs.

        :rtype: datetime.datetime
        """

    @abstractmethod
    def get_all_jobs(self):
        """
        Returns a list of all contained jobs (sorted by next run time).

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
