"""
Stores jobs in a file governed by the :mod:`shelve` module.
"""

import shelve
import pickle
import random
import logging

from apscheduler.jobstores.base import JobStore
from apscheduler.job import Job
from apscheduler.util import itervalues

logger = logging.getLogger(__name__)


class ShelveJobStore(JobStore):
    MAX_ID = 1000000

    def __init__(self, path, pickle_protocol=pickle.HIGHEST_PROTOCOL):
        self.jobs = []
        self.path = path
        self.pickle_protocol = pickle_protocol
        self._open_store()

    def _open_store(self):
        self.store = shelve.open(self.path, 'c', self.pickle_protocol)

    def _generate_id(self):
        id = None
        while not id:
            id = str(random.randint(1, self.MAX_ID))
            if not id in self.store:
                return id

    def add_job(self, job):
        job.id = self._generate_id()
        self.store[job.id] = job.__getstate__()
        self.store.close()
        self._open_store()
        self.jobs.append(job)

    def update_job(self, job):
        job_dict = self.store[job.id]
        job_dict['next_run_time'] = job.next_run_time
        job_dict['runs'] = job.runs
        self.store[job.id] = job_dict
        self.store.close()
        self._open_store()

    def remove_job(self, job):
        del self.store[job.id]
        self.store.close()
        self._open_store()
        self.jobs.remove(job)

    def load_jobs(self):
        jobs = []
        for job_dict in itervalues(self.store):
            try:
                job = Job.__new__(Job)
                job.__setstate__(job_dict)
                jobs.append(job)
            except Exception:
                job_name = job_dict.get('name', '(unknown)')
                logger.exception('Unable to restore job "%s"', job_name)

        self.jobs = jobs

    def close(self):
        self.store.close()

    def __repr__(self):
        return '<%s (path=%s)>' % (self.__class__.__name__, self.path)
