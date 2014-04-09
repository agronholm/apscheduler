"""
Stores jobs in an array in RAM. Provides no persistence support.
"""
from __future__ import absolute_import

from apscheduler.jobstores.base import BaseJobStore, JobLookupError, ConflictingIdError


class MemoryJobStore(BaseJobStore):
    def __init__(self):
        super(MemoryJobStore, self).__init__()
        self._jobs = []  # sorted by next_run_time
        self._jobs_index = {}  # id -> job lookup table

    def lookup_job(self, id):
        try:
            return self._jobs_index[id]
        except KeyError:
            raise JobLookupError(id)

    def get_pending_jobs(self, now):
        pending = []
        for job in self._jobs:
            if job.next_run_time and job.next_run_time <= now:
                pending.append(job)

        return pending

    def get_next_run_time(self):
        return self._jobs[0].next_run_time if self._jobs else None

    def get_all_jobs(self):
        return list(self._jobs)

    def add_job(self, job):
        if job.id in self._jobs_index:
            raise ConflictingIdError(job.id)

        index = self._bisect_job(job.next_run_time)
        self._jobs.insert(index, job)
        self._jobs_index[job.id] = job

    def update_job(self, job):
        old_job = self.lookup_job(job.id)
        old_index = self._get_job_index(old_job)
        self._jobs_index[old_job.id] = job

        # If the next run time has not changed, simply replace the job in its present index.
        # Otherwise, reinsert the job to the list to preserve the ordering.
        if old_job.next_run_time == job.next_run_time:
            self._jobs[old_index] = job
        else:
            del self._jobs[old_index]
            index = self._bisect_job(job.next_run_time)
            self._jobs.insert(index, job)

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
