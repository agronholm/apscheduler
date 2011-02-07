"""
Stores jobs in a file governed by the :mod:`shelve` module.
"""

import shelve
import pickle
import random

from apscheduler.jobstores.base import JobStore
from apscheduler.util import obj_to_ref, dict_values


class ShelveJobStore(JobStore):
    MAX_ID = 1000000

    def __init__(self, path, pickle_protocol=pickle.HIGHEST_PROTOCOL):
        self.jobs = []
        self.path = path
        self.pickle_protocol = pickle_protocol
        self.store = shelve.open(path, 'c', self.pickle_protocol)

    def _generate_id(self):
        id = None
        while not id:
            id = str(random.randint(1, self.MAX_ID))
            if not id in self.store:
                return id

    def add_job(self, job):
        job.func_ref = obj_to_ref(job.func)
        job.id = self._generate_id()
        self.jobs.append(job)
        self.store[job.id] = job

    def update_job(self, job):
        self.store[job.id] = job

    def remove_job(self, job):
        del self.store[job.id]
        self.jobs.remove(job)

    def load_jobs(self):
        self.jobs = dict_values(self.store)

    def close(self):
        self.store.close()

    def __repr__(self):
        return '<%s (path=%s)>' % (self.__class__.__name__, self.path)
