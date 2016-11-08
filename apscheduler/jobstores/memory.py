from __future__ import absolute_import

from apscheduler.jobstores.base import BaseJobStore, JobLookupError, ConflictingIdError
from apscheduler.util import datetime_to_utc_timestamp
import six
import datetime

class MemoryJobStore(BaseJobStore):
    """
    Stores jobs in an array in RAM. Provides no persistence support.

    Plugin alias: ``memory``
    """

    def __init__(self):
        super(MemoryJobStore, self).__init__()
        # list of (job, timestamp), sorted by next_run_time and job id (ascending)
        self._jobs = []
        self._jobs_index = {}  # id -> (job, timestamp) lookup table
        self._job_submissions = {}# id -> job_submission
        self._next_id = 1

    def add_job_submission(self, job, now):
        job_submission = {
            'id': self._next_id,
            'state': 'submitted',
            # TODO: Pickle the 'job.func' so we can recover from 2 diff sessions
            'func': job.func if isinstance(job.func, six.string_types) else job.func.__name__,
            'submitted_at': now,
            'apscheduler_job_id': job.id,
        }
        self._job_submissions[self._next_id] = job_submission
        self._next_id += 1
        return job_submission['id']

    def update_job_submissions(self, conditions, **kwargs):
        for _id in self._job_submissions:
            this_job_submission = self._job_submissions[_id]
            update_flag = True
            # If no conditions are given, all job_submissions will be updated
            for column in conditions:
                val = conditions[column]
                if this_job_submission[column] != val:
                    update_flag = False            
            if update_flag:
                this_job_submission.update(kwargs)
    
    def update_job_submission(self, job_submission_id, **kwargs):
        self._job_submissions[job_submission_id].update(kwargs)
       
    def get_job_submissions_with_states(self, states=[]):
        return [self._job_submissions[k] \
                for k in self._job_submissions \
                if not states or self._job_submissions[k]['state'] in states]

    def get_job_submission(self, job_submission_id):
        return self._job_submissions[job_submission_id]

    def lookup_job(self, job_id):
        return self._jobs_index.get(job_id, (None, None))[0]

    def get_due_jobs(self, now):
        now_timestamp = datetime_to_utc_timestamp(now)
        pending = []
        for job, timestamp in self._jobs:
            if timestamp is None or timestamp > now_timestamp:
                break
            pending.append(job)

        return pending

    def get_next_run_time(self):
        return self._jobs[0][0].next_run_time if self._jobs else None

    def get_all_jobs(self):
        return [j[0] for j in self._jobs]

    def add_job(self, job):
        if job.id in self._jobs_index:
            raise ConflictingIdError(job.id)

        timestamp = datetime_to_utc_timestamp(job.next_run_time)
        index = self._get_job_index(timestamp, job.id)
        self._jobs.insert(index, (job, timestamp))
        self._jobs_index[job.id] = (job, timestamp)

    def update_job(self, job):
        old_job, old_timestamp = self._jobs_index.get(job.id, (None, None))
        if old_job is None:
            raise JobLookupError(job.id)

        # If the next run time has not changed, simply replace the job in its present index.
        # Otherwise, reinsert the job to the list to preserve the ordering.
        old_index = self._get_job_index(old_timestamp, old_job.id)
        new_timestamp = datetime_to_utc_timestamp(job.next_run_time)
        if old_timestamp == new_timestamp:
            self._jobs[old_index] = (job, new_timestamp)
        else:
            del self._jobs[old_index]
            new_index = self._get_job_index(new_timestamp, job.id)
            self._jobs.insert(new_index, (job, new_timestamp))

        self._jobs_index[old_job.id] = (job, new_timestamp)

    def remove_job(self, job_id):
        job, timestamp = self._jobs_index.get(job_id, (None, None))
        if job is None:
            raise JobLookupError(job_id)

        index = self._get_job_index(timestamp, job_id)
        del self._jobs[index]
        del self._jobs_index[job.id]

    def remove_all_jobs(self):
        self._jobs = []
        self._jobs_index = {}

    def shutdown(self):
        self.remove_all_jobs()

    def _get_job_index(self, timestamp, job_id):
        """
        Returns the index of the given job, or if it's not found, the index where the job should be
        inserted based on the given timestamp.

        :type timestamp: int
        :type job_id: str

        """
        lo, hi = 0, len(self._jobs)
        timestamp = float('inf') if timestamp is None else timestamp
        while lo < hi:
            mid = (lo + hi) // 2
            mid_job, mid_timestamp = self._jobs[mid]
            mid_timestamp = float('inf') if mid_timestamp is None else mid_timestamp
            if mid_timestamp > timestamp:
                hi = mid
            elif mid_timestamp < timestamp:
                lo = mid + 1
            elif mid_job.id > job_id:
                hi = mid
            elif mid_job.id < job_id:
                lo = mid + 1
            else:
                return mid

        return lo
