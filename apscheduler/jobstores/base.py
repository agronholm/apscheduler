"""
Abstract base class that provides the interface needed by all job stores.
Job store methods are also documented here.
"""

from threading import Lock
from datetime import datetime
import random


class JobStore(object):
    stores_transient = False
    stores_persistent = False

    @property
    def default_alias(self):
        clsname = self.__class__.__name__.lower()
        if clsname.endswith('jobstore'):
            return clsname[:-8]
        raise NotImplementedError('No default alias defined')

    def add_job(self, jobmeta):
        """Adds the given job from this store."""
        raise NotImplementedError

    def remove_job(self, jobmeta):
        """Removes the given jobs from this store."""
        raise NotImplementedError

    def checkout_jobs(self, end_time):
        """
        Checks out the currently pending jobs for execution.
        The job store's responsibility is to mark the job as running and
        set a new run time for the job using the job's trigger.
        
        :param end_time: current time, used to filter out jobs that aren't
            supposed to be run yet
        :type end_time: :class:`datetime.datetime`
        :return: list of pending jobs
        """
        raise NotImplementedError

    def checkin_job(self, jobmeta):
        """
        Updates the persistent record of the given job and increments its run
        counter.
        """
        raise NotImplementedError

    def list_jobs(self):
        """
        Retrieves a list of jobs stored in this store. This list may not be
        used to modify the contents of the store, so it must be a copy.
        """
        raise NotImplementedError

    def get_next_run_time(self, start_time):
        """
        Returns the earliest time that a job from this job store is supposed to
        be run.
        """
        raise NotImplementedError

    def str(self):
        return '%s (%s)' % (self.alias, self.__class__.__name__)


def store(func):
    """
    Wrapper for methods of DictJobStore that acquires the lock and opens the
    backend at the beginning of the invocation and closes/unlocks at the end.
    """
    def wrapper(self, *args, **kwargs):
        self._lock.acquire()
        store = self._open_store()
        try:
            return func(self, store, *args, **kwargs)
        finally:
            self._close_store(store)
            self._lock.release()
    return wrapper


class DictJobStore(JobStore):
    """
    Base class for job stores backed by a dict-like object.
    """
    MAX_ID = 1000000

    def __init__(self):
        self._lock = Lock()

    def _generate_id(self, store):
        id = None
        while not id:
            id = str(random.randint(1, self.MAX_ID))
            if not id in store:
                return id

    @store
    def add_job(self, store, jobmeta):
        jobmeta.id = self._generate_id(store)
        jobmeta.jobstore = self
        self._put_jobmeta(store, jobmeta)

    @store
    def remove_job(self, store, jobmeta):
        store.pop(jobmeta.id, None)

    @store
    def checkout_jobs(self, store, end_time):
        jobmetas = []
        now = datetime.now()
        for jobmeta in store.values():
            if jobmeta.next_run_time <= end_time and not jobmeta.checkout_time:
                jobmetas.append(self._export_jobmeta(jobmeta))

                # Mark this job as started and compute the next run time
                jobmeta.checkout_time = now
                jobmeta.next_run_time = jobmeta.trigger.get_next_fire_time(now)
                if jobmeta.next_run_time:
                    self._put_jobmeta(store, jobmeta)
                else:
                    store.pop(jobmeta.id)
        return jobmetas

    @store
    def checkin_job(self, store, jobmeta):
        storedmeta = store[jobmeta.id]
        storedmeta.job = jobmeta.job
        storedmeta.checkout_time = None
        self._put_jobmeta(store, storedmeta)

    @store
    def list_jobs(self, store):
        return [self._export_jobmeta(jm) for jm in store.values()]

    @store
    def get_next_run_time(self, store, start_time):
        next_run_time = None
        for jobmeta in store.values():
            assert jobmeta.next_run_time
            if (jobmeta.next_run_time > start_time and (not next_run_time or
                jobmeta.next_run_time < next_run_time)):
                next_run_time = jobmeta.next_run_time
        return next_run_time

    def __repr__(self):
        return self.__class__.__name__
