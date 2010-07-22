"""
Stores jobs in a file governed by the :mod:`shelve` module.
"""

import shelve
import pickle
import random

from apscheduler.jobstore.base import JobStore


class ShelveJobStore(JobStore):
    MAX_ID = 1000000

    stores_persistent = True

    def __init__(self, path, protocol=pickle.HIGHEST_PROTOCOL):
        self.path = path
        self.protocol = protocol

    def _open_shelve(self, mode):
        return shelve.open(self.path, mode, self.protocol)

    def add_job(self, job):
        job.jobstore = self
        shelve = self._open_shelve('c')
        while not job.id:
            id = str(random.randint(1, self.MAX_ID))
            if not id in shelve:
                job.id = id
                shelve[id] = job
        shelve.close()

    def update_jobs(self, jobs):
        shelve = self._open_shelve('w')
        for job in job:
            shelve[job.id] = job
        shelve.close()

    def remove_jobs(self, jobs):
        shelve = self._open_shelve('w')
        for job in jobs:
            if job.id in shelve:
                del shelve[job.id]
        shelve.close()

    def get_jobs(self, end_time=None):
        jobs = []
        shelve = self._open_shelve('r')
        for job in shelve.values():
            if not end_time or job.next_run_time <= end_time:
                jobs.append(job)
        shelve.close()
        return jobs

    def str(self):
        return '%s (%s, %s)' % (self.alias, self.__class__.__name__,
                                self.path)
