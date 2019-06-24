import logging
from abc import ABCMeta, abstractmethod

import six

from apscheduler.schedulers.base import (ConflictingIdError, JobLookupError,
                                         TransientJobError)


class BaseAsyncJobStore(six.with_metaclass(ABCMeta)):
    """Abstract base class that defines the interface that every job store must implement."""

    _scheduler = None
    _alias = None
    _logger = logging.getLogger('apscheduler.jobstores')

    async def start(self, scheduler, alias):
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

    async def shutdown(self):
        """Frees any resources still bound to this job store."""

    def _fix_paused_jobs_sorting(self, jobs):
        for i, job in enumerate(jobs):
            if job.next_run_time is not None:
                if i > 0:
                    paused_jobs = jobs[:i]
                    del jobs[:i]
                    jobs.extend(paused_jobs)
                break

    @abstractmethod
    async def lookup_job(self, job_id):
        """
        Returns a specific job, or ``None`` if it isn't found..

        The job store is responsible for setting the ``scheduler`` and ``jobstore`` attributes of
        the returned job to point to the scheduler and itself, respectively.

        :param str|unicode job_id: identifier of the job
        :rtype: Job
        """

    @abstractmethod
    async def get_due_jobs(self, now):
        """
        Returns the list of jobs that have ``next_run_time`` earlier or equal to ``now``.
        The returned jobs must be sorted by next run time (ascending).

        :param datetime.datetime now: the current (timezone aware) datetime
        :rtype: list[Job]
        """

    @abstractmethod
    async def get_next_run_time(self):
        """
        Returns the earliest run time of all the jobs stored in this job store, or ``None`` if
        there are no active jobs.

        :rtype: datetime.datetime
        """

    @abstractmethod
    async def get_all_jobs(self):
        """
        Returns a list of all jobs in this job store.
        The returned jobs should be sorted by next run time (ascending).
        Paused jobs (next_run_time == None) should be sorted last.

        The job store is responsible for setting the ``scheduler`` and ``jobstore`` attributes of
        the returned jobs to point to the scheduler and itself, respectively.

        :rtype: list[Job]
        """

    @abstractmethod
    async def add_job(self, job):
        """
        Adds the given job to this store.

        :param Job job: the job to add
        :raises ConflictingIdError: if there is another job in this store with the same ID
        """

    @abstractmethod
    async def update_job(self, job):
        """
        Replaces the job in the store with the given newer version.

        :param Job job: the job to update
        :raises JobLookupError: if the job does not exist
        """

    @abstractmethod
    async def remove_job(self, job_id):
        """
        Removes the given job from this store.

        :param str|unicode job_id: identifier of the job
        :raises JobLookupError: if the job does not exist
        """

    @abstractmethod
    async def remove_all_jobs(self):
        """Removes all jobs from this store."""

    def __repr__(self):
        return '<%s>' % self.__class__.__name__
