"""
Stores jobs in an array in RAM. Provides no persistence support.
"""
from __future__ import absolute_import

import six

from apscheduler.jobstores.base import BaseJobStore, JobLookupError, ConflictingIdError


class MemoryJobStore(BaseJobStore):
    def __init__(self, scheduler):
        super(MemoryJobStore, self).__init__()
        self._jobs = []  # sorted by next_run_time
        self._jobs_index = {}  # id -> job lookup table

    def lookup_job(self, id):
        try:
            return self._jobs_index[id]
        except KeyError:
            raise JobLookupError(id)

    def get_pending_jobs(self, now):
        index, end = 0, len(self._jobs)
        pending_jobs = []
        next_run_time = None
        while index < end:
            job = self._jobs[index]
            if job.next_run_time <= now:
                pending_jobs.append(job)
                index += 1
            else:
                next_run_time = job.next_run_time
                break

        return pending_jobs, next_run_time

    def get_all_jobs(self):
        return list(self._jobs)

    def add_job(self, job):
        if job.id in self._jobs_index:
            raise ConflictingIdError(job.id)

        index = self._bisect_job(job.next_run_time)
        self._jobs.insert(index, job)
        self._jobs_index[job.id] = job

    def update_job(self, id, changes):
        job = self.lookup_job(id)

        # If the job identifier changed, need to update the index
        if 'id' in changes and changes['id'] != job.id:
            del self._jobs_index[job.id]
            if changes['id'] in self._jobs_index:
                raise ConflictingIdError(changes['id'])
            self._jobs_index[changes['id']] = job

        # Need to update the location of the job in the list if the next run time changes
        if 'next_run_time' in changes:
            index = self._get_job_index(job)
            del self._jobs[index]
            index = self._bisect_job(changes['next_run_time'])
            self._jobs.insert(index, job)

        for key, value in six.iteritems(changes):
            setattr(job, key, value)

    def remove_job(self, id):
        job = self.lookup_job(id)
        index = self._get_job_index(job)
        del self._jobs[index]
        del self._jobs_index[job.id]

    def remove_all_jobs(self):
        self._jobs = []
        self._jobs_index = {}

    def close(self):
        self.remove_all_jobs()

    def _get_job_index(self, job):
        jobs = self._jobs
        index = self._bisect_job(job.next_run_time)
        end = len(self._jobs)
        while index < end:
            if jobs[index].id == job.id:
                return index

    def _bisect_job(self, run_time):
        # Adapted from the bisect module
        jobs = self._jobs
        lo, hi = 0, len(jobs)
        while lo < hi:
            mid = (lo + hi) // 2
            if (jobs[mid].next_run_time is None and run_time is not None) or \
                    (run_time is not None and jobs[mid].next_run_time < run_time):
                lo = mid + 1
            else:
                hi = mid

        return lo
