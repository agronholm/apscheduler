"""
Stores jobs in an array in RAM. Provides no persistence support.
"""

from apscheduler.jobstore.base import JobStore


class RAMJobStore(JobStore):
    stores_transient = True

    def __init__(self, **options):
        self.jobs = []

    def add_job(self, job):
        job.jobstore = self
        self.jobs.append(job)

    def update_jobs(self, jobs):
        pass

    def remove_jobs(self, jobs):
        for job in jobs:
            self.jobs.remove(job)

    def get_jobs(self, end_time=None):
        if end_time:
            return [j for j in self.jobs if j.next_run_time <= end_time]
        else:
            return list(self.jobs)
