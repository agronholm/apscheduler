"""
Jobs represent scheduled tasks.
"""

import logging

from apscheduler.util import obj_to_ref, ref_to_obj, get_callable_name


logger = logging.getLogger(__name__)

class Job(object):
    """
    Abstract base class for jobs. Custom stateful jobs should inherit from this
    class.
    """

    id = None
    jobstore = None
    next_run_time = None
    num_runs = 0

    def __init__(self, trigger, name=None, max_runs=None,
                 misfire_grace_time=1):
        """
        :param trigger: trigger for the given callable
        :param name: name of the job (optional)
        :param max_runs: maximum number of times this job is allowed to be
            triggered
        :param misfire_grace_time: seconds after the designated run time that
            the job is still allowed to be run
        """
        self.trigger = trigger
        self.max_runs = max_runs
        self.misfire_grace_time = misfire_grace_time
        self.name = name or '(unnamed)'

        if not self.trigger:
            raise ValueError('The job must have a trigger')
        if self.max_runs is not None and self.max_runs <= 0:
            raise ValueError('max_runs must be a positive value or None')
        if self.misfire_grace_time <= 0:
            raise ValueError('misfire_grace_time must be a positive value')

    def run(self):
        """
        Runs the job. This method is executed in a separate thread.
        Subclasses should override this.
        """
        raise NotImplementedError

    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop('jobstore', None)
        return state

    def __eq__(self, job):
        if isinstance(job, Job):
            if self.id is not None:
                return job.id == self.id
            return self is job
        return False

    def __repr__(self):
        return '%s: %s' % (self.name, self.trigger)


class SimpleJob(Job):
    """
    Job that runs the given function with the given arguments when triggered.
    """

    def __init__(self, trigger, func, args=None, kwargs=None, name=None,
                 **job_options):
        """
        :param trigger: trigger for the given callable
        :param func: callable to call when the trigger is triggered
        :param args: list of positional arguments to call func with
        :param kwargs: dict of keyword arguments to call func with
        :param name: name of the job (if none specified, defaults to the name
            of the function)
        """
        if not hasattr(func, '__call__'):
            raise TypeError('func must be callable')

        self.func = func
        self.args = args or []
        self.kwargs = kwargs or {}
        name = name or get_callable_name(func)
        Job.__init__(self, trigger, name=name, **job_options)

    def run(self):
        """
        Runs the associated callable.
        This method is executed in a separate thread.
        """
        try:
            self.func(*self.args, **self.kwargs)
        except:
            logger.exception('Error executing job "%s"', self)
            raise

    def __getstate__(self):
        state = Job.__getstate__(self)
        state['func'] = obj_to_ref(state['func'])
        return state

    def __setstate__(self, state):
        state['func'] = ref_to_obj(state['func'])
        self.__dict__.update(state)
