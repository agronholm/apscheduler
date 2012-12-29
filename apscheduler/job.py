"""
Jobs represent scheduled tasks.
"""

from threading import Lock
from datetime import timedelta

from apscheduler.util import to_unicode, ref_to_obj, get_callable_name,\
    obj_to_ref


class MaxInstancesReachedError(Exception):
    pass


class Job(object):
    """
    Encapsulates the actual Job along with its metadata. Job instances
    are created by the scheduler when adding jobs, and should not be
    directly instantiated. These options can be set when adding jobs
    to the scheduler (see :ref:`job_options`).

    :var trigger: trigger that determines the execution times
    :var func: callable to call when the trigger is triggered
    :var args: list of positional arguments to call func with
    :var kwargs: dict of keyword arguments to call func with
    :var name: name of the job
    :var misfire_grace_time: seconds after the designated run time that
        the job is still allowed to be run
    :var coalesce: run once instead of many times if the scheduler determines
        that the job should be run more than once in succession
    :var max_runs: maximum number of times this job is allowed to be
        triggered
    :var max_instances: maximum number of concurrently running
        instances allowed for this job
    :var runs: number of times this job has been triggered
    :var instances: number of concurrently running instances of this job
    """
    id = None
    next_run_time = None

    def __init__(self, trigger, func, args, kwargs, misfire_grace_time,
                 coalesce, name=None, max_runs=None, max_instances=1):
        if not trigger:
            raise ValueError('The trigger must not be None')
        if not hasattr(func, '__call__'):
            raise TypeError('func must be callable')
        if not hasattr(args, '__getitem__'):
            raise TypeError('args must be a list-like object')
        if not hasattr(kwargs, '__getitem__'):
            raise TypeError('kwargs must be a dict-like object')
        if misfire_grace_time <= 0:
            raise ValueError('misfire_grace_time must be a positive value')
        if max_runs is not None and max_runs <= 0:
            raise ValueError('max_runs must be a positive value')
        if max_instances <= 0:
            raise ValueError('max_instances must be a positive value')

        self._lock = Lock()

        self.trigger = trigger
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.name = to_unicode(name or get_callable_name(func))
        self.misfire_grace_time = misfire_grace_time
        self.coalesce = coalesce
        self.max_runs = max_runs
        self.max_instances = max_instances
        self.runs = 0
        self.instances = 0

    def compute_next_run_time(self, now):
        if self.runs == self.max_runs:
            self.next_run_time = None
        else:
            self.next_run_time = self.trigger.get_next_fire_time(now)

        return self.next_run_time

    def get_run_times(self, now):
        """
        Computes the scheduled run times between ``next_run_time`` and ``now``.
        """
        run_times = []
        run_time = self.next_run_time
        increment = timedelta(microseconds=1)
        while ((not self.max_runs or self.runs < self.max_runs) and
               run_time and run_time <= now):
            run_times.append(run_time)
            run_time = self.trigger.get_next_fire_time(run_time + increment)

        return run_times

    def add_instance(self):
        self._lock.acquire()
        try:
            if self.instances == self.max_instances:
                raise MaxInstancesReachedError
            self.instances += 1
        finally:
            self._lock.release()

    def remove_instance(self):
        self._lock.acquire()
        try:
            assert self.instances > 0, 'Already at 0 instances'
            self.instances -= 1
        finally:
            self._lock.release()

    def __getstate__(self):
        # Prevents the unwanted pickling of transient or unpicklable variables
        state = self.__dict__.copy()
        state.pop('instances', None)
        state.pop('func', None)
        state.pop('_lock', None)
        state['func_ref'] = obj_to_ref(self.func)
        return state

    def __setstate__(self, state):
        state['instances'] = 0
        state['func'] = ref_to_obj(state.pop('func_ref'))
        state['_lock'] = Lock()
        self.__dict__ = state

    def __eq__(self, other):
        if isinstance(other, Job):
            return self.id is not None and other.id == self.id or self is other
        return NotImplemented

    def __repr__(self):
        return '<Job (name=%s, trigger=%s)>' % (self.name, repr(self.trigger))

    def __str__(self):
        return '%s (trigger: %s, next run at: %s)' % (
            self.name, str(self.trigger), str(self.next_run_time))
