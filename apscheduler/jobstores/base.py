"""
Abstract base class that provides the interface needed by all job stores.
Job store methods are also documented here.
"""


class JobStore(object):
    def add_job(self, job):
        """Adds the given job from this store."""
        raise NotImplementedError

    def update_job(self, job):
        """Persists the running state of the given job."""
        raise NotImplementedError

    def remove_job(self, job):
        """Removes the given jobs from this store."""
        raise NotImplementedError

    def load_jobs(self):
        """Loads jobs from this store into memory."""
        raise NotImplementedError

    def close(self):
        """Frees any resources still bound to this job store."""
