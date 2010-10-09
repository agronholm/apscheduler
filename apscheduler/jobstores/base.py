"""
Abstract base class that provides the interface needed by all job stores.
Job store methods are also documented here.
"""

class JobStore(object):
    @property
    def default_alias(self):
        clsname = self.__class__.__name__.lower()
        if clsname.endswith('jobstore'):
            return clsname[:-8]
        raise NotImplementedError('No default alias defined')

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

    def str(self):
        return '%s (%s)' % (self.alias, self.__class__.__name__)
