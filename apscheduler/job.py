"""
Jobs represent scheduled tasks.
"""

import logging

from apscheduler.util import obj_to_ref, ref_to_obj, to_unicode


logger = logging.getLogger(__name__)

class JobMeta(object):
    """
    Encapsulates the actual Job along with its metadata. JobMeta instances
    are created by the scheduler when adding jobs, and it should not be
    directly instantiated.

    :param job: the job object (contains the "run" method)
    :param trigger: trigger that determines the execution times of the
        enclosed job
    :param name: name of the job (optional)
    :param max_runs: maximum number of times this job is allowed to be
        triggered
    :param misfire_grace_time: seconds after the designated run time that
        the job is still allowed to be run
    """

    id = None
    jobstore = None
    next_run_time = None
    checkout_time = None

    def __init__(self, job, trigger, name=None, misfire_grace_time=1):
        self.job = job
        self.trigger = trigger
        self.name = name
        self.misfire_grace_time = misfire_grace_time

        if not self.trigger:
            raise ValueError('The trigger must not be None')
        if not self.job:
            raise ValueError('The job must not be None')
        if name:
            self.name = to_unicode(name)
        if self.misfire_grace_time <= 0:
            raise ValueError('misfire_grace_time must be a positive value')

    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop('jobstore', None)
        return state

    def __cmp__(self, other):
        return cmp(self.next_run_time, other.next_run_time)

    def __eq__(self, other):
        if isinstance(other, JobMeta):
            if self.id is not None:
                return other.id == self.id
            return self is other
        return False

    def __repr__(self):
        return '%s: %s' % (self.name, self.trigger)


class SimpleJob(object):
    """
    Job that runs the given function with the given arguments when triggered.
    These are instantiated by the scheduler's shortcut methods and it should
    not be necessary to create these directly.

    :param func: callable to call when the trigger is triggered
    :param args: list of positional arguments to call func with
    :param kwargs: dict of keyword arguments to call func with
    """

    def __init__(self, func, args=None, kwargs=None):
        if not hasattr(func, '__call__'):
            raise TypeError('func must be callable')

        self.func = func
        self.args = args or []
        self.kwargs = kwargs or {}

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
        state = self.__dict__.copy()
        state['func'] = obj_to_ref(state['func'])
        return state

    def __setstate__(self, state):
        state['func'] = ref_to_obj(state['func'])
        self.__dict__.update(state)
