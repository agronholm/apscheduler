"""
Stores jobs in a file governed by the :mod:`shelve` module.
"""

import shelve
import pickle
import random

from apscheduler.jobstores.base import JobStore
from apscheduler.util import obj_to_ref, dict_values


def store(func):
    def wrapper(self, *args, **kwargs):
        store = shelve.open(self.path, 'c', self.pickle_protocol)
        try:
            return func(self, store, *args, **kwargs)
        finally:
            store.close()
    return wrapper


class ShelveJobStore(JobStore):
    MAX_ID = 1000000

    def __init__(self, path, pickle_protocol=pickle.HIGHEST_PROTOCOL):
        self.jobs = []
        self.path = path
        self.pickle_protocol = pickle_protocol

    def _generate_id(self, store):
        id = None
        while not id:
            id = str(random.randint(1, self.MAX_ID))
            if not id in store:
                return id

    @store
    def add_job(self, store, job):
        job.func_ref = obj_to_ref(job.func)
        job.id = self._generate_id(store)
        self.jobs.append(job)
        store[job.id] = job

    @store
    def update_job(self, store, job):
        store[job.id] = job

    @store
    def remove_job(self, store, job):
        del store[job.id]
        self.jobs.remove(job)

    @store
    def load_jobs(self, store):
        self.jobs = dict_values(store)

    def __repr__(self):
        return '<%s (path=%s)>' % (self.__class__.__name__, self.path)
