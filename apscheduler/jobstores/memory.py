"""
Stores jobs in an array in RAM. Provides no persistence support.
"""
from __future__ import absolute_import

from apscheduler.jobstores.base import JobStore


class MemoryJobStore(JobStore):
    def __init__(self):
        self.jobs = []

    def add_job(self, job):
        self.jobs.append(job)

    def update_job(self, job):
        pass

    def remove_job(self, job):
        self.jobs.remove(job)

    def load_jobs(self):
        pass

    def __repr__(self):
        return '<%s>' % (self.__class__.__name__)
