"""
Jobs represent scheduled tasks.
"""

from threading import Lock
from datetime import timedelta

from six import u

from apscheduler.util import ref_to_obj, obj_to_ref, get_callable_name, datetime_repr


class MaxInstancesReachedError(Exception):
    pass


class Job(object):
    """
    Encapsulates the actual Job along with its metadata. Job instances are created by the scheduler when adding jobs,
    and should not be directly instantiated.
    """
    id = None
    next_run_time = None

    def __init__(self, trigger, func, args, kwargs, misfire_grace_time, coalesce, name, max_runs, max_instances):
        if isinstance(func, str):
            self.func = ref_to_obj(func)
            self.func_ref = func
        elif callable(func):
            self.func = func
            try:
                self.func_ref = obj_to_ref(func)
            except ValueError:
                # If this happens, this Job won't be serializable
                self.func_ref = None
        else:
            raise TypeError('func must be a callable or a textual reference to one')

        self._lock = Lock()

        self.trigger = trigger
        self.args = args
        self.kwargs = kwargs
        self.name = u(name or get_callable_name(self.func))
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
        while (not self.max_runs or self.runs < self.max_runs) and run_time and run_time <= now:
            run_times.append(run_time)
            run_time = self.trigger.get_next_fire_time(run_time + increment)

        return run_times

    def add_instance(self):
        with self._lock:
            if self.instances == self.max_instances:
                raise MaxInstancesReachedError
            self.instances += 1

    def remove_instance(self):
        with self._lock:
            assert self.instances > 0, 'Already at 0 instances'
            self.instances -= 1

    def __getstate__(self):
        # Prevents the unwanted pickling of transient or unpicklable variables
        state = self.__dict__.copy()
        state.pop('instances', None)
        state.pop('func', None)
        state.pop('_lock', None)
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
        return '<Job (name=%s, trigger=%r)>' % (self.name, self.trigger)

    def __str__(self):
        return '%s (trigger: %s, next run at: %s)' % (self.name, self.trigger, datetime_repr(self.next_run_time))
