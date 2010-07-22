"""
Abstract base class that provides the interface needed by all job stores.
Job store methods are also documented here.
"""

class JobStore(object):
    stores_transient = False
    stores_persistent = False

    @property
    def alias(self):
        clsname = self.__class__.__name__.lower()
        if clsname.endswith('jobstore'):
            return clsname[:-8]
        raise NotImplementedError('No default alias defined')

    def add_job(self, job):
        """Adds the given job from this store."""
        raise NotImplementedError

    def update_jobs(self, jobs):
        """Updates the persistent record of the given Jobs."""
        raise NotImplementedError

    def remove_jobs(self, jobs):
        """Removes the given job from this store."""
        raise NotImplementedError

    def get_jobs(self, end_time=None):
        """
        Retrieves jobs scheduled in this store."
        
        :param start_time: if specified, filter jobs so that only the ones
            whose next run time (+ misfire_grace_time) must be earlier or equal
            to end_time. 
        :type start_time: :class:`datetime.datetime`
        :return: list of Jobs matching the filter, if any
        """
        raise NotImplementedError

    def str(self):
        return '%s (%s)' % (self.alias, self.__class__.__name__)
