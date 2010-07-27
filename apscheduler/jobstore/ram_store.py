"""
Stores jobs in an array in RAM. Provides no persistence support.
"""

from copy import copy

from apscheduler.jobstore.base import DictJobStore


class RAMJobStore(DictJobStore):
    stores_transient = True

    def __init__(self):
        DictJobStore.__init__(self)
        self._jobmetas = {}

    def _open_store(self):
        return self._jobmetas

    def _close_store(self, store):
        pass

    def _export_jobmeta(self, jobmeta):
        jobmeta = copy(jobmeta)
        jobmeta.jobstore = self
        return jobmeta

    def _put_jobmeta(self, store, jobmeta):
        store[jobmeta.id] = copy(jobmeta)
