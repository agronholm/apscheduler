"""
Jobs represent scheduled tasks.
"""

from datetime import datetime
import logging

from apscheduler.util import obj_to_ref, ref_to_obj, get_callable_name


logger = logging.getLogger(__name__)

class Job(object):
    id = None
    jobstore = None

    def __init__(self, trigger, func, args=None, kwargs=None, name=None,
                 misfire_grace_time=None):
        """
        :param trigger: trigger for the given callable
        :param func: callable to call when the trigger is triggered
        :param args: list of positional arguments to call func with
        :param kwargs: dict of keyword arguments to call func with
        :param name: name of the job (if none specified, defaults to the name
            of the function)
        :param misfire_grace_time: seconds after the designated run time that
            the job is still allowed to be run
        """
        if not hasattr(func, '__call__'):
            raise TypeError('func must be callable')

        self.trigger = trigger
        self.func = func
        self.args = args or []
        self.kwargs = kwargs or {}
        self.misfire_grace_time = misfire_grace_time
        self.name = name or get_callable_name(func)
        self.next_run_time = trigger.get_next_fire_time(datetime.now())

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

    def __eq__(self, job):
        if isinstance(job, Job):
            if self.id is not None:
                return job.id == self.id
            return self is job
        return False

    def __repr__(self):
        return '%s: %s' % (self.name or '(unnamed)', self.trigger)
