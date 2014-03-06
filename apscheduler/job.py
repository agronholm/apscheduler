"""
Jobs represent scheduled tasks.
"""

from threading import Lock
from datetime import timedelta
from uuid import uuid4

import six

from apscheduler.util import ref_to_obj, obj_to_ref, get_callable_name, datetime_repr


class MaxInstancesReachedError(Exception):
    pass


class NoSchedulerAttachedError(Exception):
    def __init__(self):
        super(Exception, self).__init__('This job has no scheduler attached to it')


class Job(object):
    """
    Encapsulates the actual Job along with its metadata. Job instances are created by the scheduler when adding jobs,
    and should not be directly instantiated.
    """

    __slots__ = ('_lock', 'scheduler', 'jobstore', 'id', 'func', 'func_ref', 'trigger', 'args', 'kwargs', 'name',
                 'misfire_grace_time', 'coalesce', 'max_runs', 'max_instances', 'runs', 'instances', 'next_run_time')

    def __init__(self, trigger, func, args, kwargs, id, misfire_grace_time, coalesce, name,
                 max_runs, max_instances):
        if isinstance(func, six.string_types):
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
        self.id = id or uuid4().hex
        self.trigger = trigger
        self.args = args
        self.kwargs = kwargs
        self.name = six.u(name or get_callable_name(self.func))
        self.misfire_grace_time = misfire_grace_time
        self.coalesce = coalesce
        self.max_runs = max_runs
        self.max_instances = max_instances
        self.runs = 0
        self.instances = 0
        self.next_run_time = None

    #
    # Public API
    #

    def remove(self):
        if not hasattr(self, 'scheduler'):
            raise NoSchedulerAttachedError
        self.scheduler.unschedule_job(self.id, self.jobstore)

    #
    # Protected API
    #

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

    def attach_scheduler(self, scheduler, jobstore):
        self.scheduler = scheduler
        self.jobstore = jobstore

    def __getstate__(self):
        return {
            'version': 1,
            'id': self.id,
            'func_ref': self.func_ref,
            'trigger': self.trigger,
            'args': self.args,
            'kwargs': self.kwargs,
            'name': self.name,
            'misfire_grace_time': self.misfire_grace_time,
            'coalesce': self.coalesce,
            'max_runs': self.max_runs,
            'max_instances': self.max_instances,
            'runs': self.runs,
            'next_run_time': self.next_run_time,
        }

    def __setstate__(self, state):
        if state.get('version', 1) > 1:
            raise ValueError('Job has version %s, but only version 1 and lower can be handled' % state['version'])

        self.id = state['id']
        self.func_ref = state['func_ref']
        self.trigger = state['trigger']
        self.args = state['args']
        self.kwargs = state['kwargs']
        self.name = state['name']
        self.misfire_grace_time = state['misfire_grace_time']
        self.coalesce = state['coalesce']
        self.max_runs = state['max_runs']
        self.max_instances = state['max_instances']
        self.runs = state['runs']
        self.next_run_time = state['next_run_time']

        self._lock = Lock()
        self.func = ref_to_obj(self.func_ref)
        self.instances = 0

    def __eq__(self, other):
        if isinstance(other, Job):
            return self.id == other.id
        return NotImplemented

    def __repr__(self):
        return '<Job (name=%s, trigger=%r)>' % (self.name, self.trigger)

    def __str__(self):
        return '%s (trigger: %s, next run at: %s)' % (self.name, self.trigger, datetime_repr(self.next_run_time))
