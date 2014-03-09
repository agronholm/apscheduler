"""
Abstract base class that provides the interface needed by all job stores.
Job store methods are also documented here.
"""
from abc import ABCMeta, abstractmethod

import six


class JobLookupError(KeyError):
    """Raised when the job store cannot find a job for update or removal."""

    def __init__(self, id):
        super(JobLookupError, self).__init__(six.u('No job by the id of %s was found') % id)


class ConflictingIdError(KeyError):
    """Raised when the uniqueness of job IDs is being violated."""

    def __init__(self, id):
        super(ConflictingIdError, self).__init__(six.u('Job identifier (%s) conflicts with an existing job') % id)


class TransientJobError(ValueError):
    """Raised when an attempt to add transient (with no func_ref) job to a persistent job store is detected."""

    def __init__(self, id):
        super(TransientJobError, self).__init__(
            six.u('Job (%s) cannot be added to this job store because a reference to the callable could not be '
                  'determined.') % id)


class BaseJobStore(six.with_metaclass(ABCMeta)):
    @abstractmethod
    def lookup_job(self, id):
        """Returns a specific job.

        :param id: identifier of the job
        :type id: str/unicode
        :rtype: :class:`~apscheduler.job.Job`
        :raises: :class:`~apscheduler.jobstore.base.JobLookupError` if the job is not found.
        """

    @abstractmethod
    def get_pending_jobs(self, now):
        """Returns a two element tuple:
           #. the list of jobs that have ``next_run_time`` earlier or equal to ``now`` (sorted by next run time)
           #. the timestamp (:class:`~datetime.datetime`) of the earliest pending job not on the list above

        :type now: :class:`~datetime.datetime` (with tzinfo)
        :rtype: :class:`list`
        """

    @abstractmethod
    def get_all_jobs(self):
        """Returns a list of all contained jobs (sorted by next run time).

        :rtype: :class:`list`
        """

    @abstractmethod
    def add_job(self, job):
        """Adds the given job to this store.

        :param job: the job to add
        :type job: :class:`~apscheduler.job.Job`
        """

    @abstractmethod
    def modify_job(self, id, changes):
        """Makes the specified changes to the given job.

        :param id: identifier of the job
        :param changes: mapping of job attribute -> value
        :type id: str/unicode
        :type changes: :class:`dict`
        :raises: :class:`~apscheduler.jobstore.base.JobLookupError` if the job does not exist.
        """

    @abstractmethod
    def remove_job(self, id):
        """Removes the given job from this store.

        :param id: identifier of the job
        :type id: str/unicode
        :raises: :class:`~apscheduler.jobstore.base.JobLookupError` if the job does not exist.
        """

    @abstractmethod
    def remove_all_jobs(self):
        """Removes all jobs from this store."""

    def close(self):
        """Frees any resources still bound to this job store."""

    def __repr__(self):
        return '<%s>' % self.__class__.__name__
