"""
Stores jobs in a file governed by the :mod:`shelve` module.
"""

from copy import copy
import shelve
import pickle

from apscheduler.jobstores.base import DictJobStore


class ShelveJobStore(DictJobStore):
    stores_persistent = True

    def __init__(self, path, protocol=pickle.HIGHEST_PROTOCOL):
        DictJobStore.__init__(self)
        self.path = path
        self.protocol = protocol

    def _open_store(self):
        return shelve.open(self.path, 'c', self.protocol)

    def _close_store(self, store):
        store.close()

    def _export_jobmeta(self, jobmeta):
        jobmeta = copy(jobmeta)
        jobmeta.jobstore = self
        return jobmeta

    def _put_jobmeta(self, store, jobmeta):
        store[jobmeta.id] = jobmeta

    def __repr__(self):
        return '%s (%s)' % (self.__class__.__name__, self.path)
