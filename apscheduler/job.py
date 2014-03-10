from threading import Lock
from datetime import timedelta
from uuid import uuid4

import six

from apscheduler.util import ref_to_obj, obj_to_ref, get_callable_name, datetime_repr, repr_escape


class MaxInstancesReachedError(Exception):
    pass


class Job(object):
    """
    Encapsulates the actual Job along with its metadata. This class is used internally by APScheduler, and should never
    be instantiated by the user.
    """

    __slots__ = ('_lock', 'id', '_func', '_func_ref', 'trigger', 'args', 'kwargs', 'name', 'misfire_grace_time',
                 'coalesce', 'max_runs', 'max_instances', 'runs', 'instances', 'next_run_time')
    modifiable_attributes = ('id', 'func', 'trigger', 'args', 'kwargs', 'name', 'misfire_grace_time', 'coalesce',
                             'max_runs', 'max_instances', 'runs', 'next_run_time')

    def __init__(self, trigger, func, args, kwargs, id, misfire_grace_time, coalesce, name,
                 max_runs, max_instances):
        self._lock = Lock()
        self.id = id or uuid4().hex
        self.func = func
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
        return {
            'version': 1,
            'id': self.id,
            'func': self._func_ref,
            'trigger': self.trigger,
            'args': self.args,
            'kwargs': self.kwargs,
            'name': self.name,
            'misfire_grace_time': self.misfire_grace_time,
            'coalesce': self.coalesce,
            'max_runs': self.max_runs,
            'max_instances': self.max_instances,
            'runs': self.runs,
            'next_run_time': self.next_run_time
        }

    def __setstate__(self, state):
        if state.get('version', 1) > 1:
            raise ValueError('Job has version %s, but only version 1 and lower can be handled' % state['version'])

        self._lock = Lock()
        self.id = state['id']
        self.func = state['func']
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
        self.instances = 0

    @property
    def func(self):
        return self._func

    @func.setter
    def func(self, value):
        if isinstance(value, six.string_types):
            self._func = ref_to_obj(value)
            self._func_ref = value
        elif callable(value):
            self._func = value
            try:
                self._func_ref = obj_to_ref(value)
            except ValueError:
                # If this happens, this Job won't be serializable
                self._func_ref = None
        else:
            raise TypeError('func must be a callable or a textual reference to one')

    def __eq__(self, other):
        if isinstance(other, Job):
            return self.id == other.id
        return NotImplemented

    def __repr__(self):
        return '<Job (id=%s)>' % repr_escape(self.id)


class JobHandle(object):
    __slots__ = ('scheduler', 'jobstore', '_job_state')

    def __init__(self, scheduler, jobstore, job):
        super(JobHandle, self).__init__()
        self.scheduler = scheduler
        self.jobstore = jobstore
        self._job_state = job.__getstate__()

    def remove(self):
        """Deletes the referenced job."""

        self.scheduler.remove_job(self.id, self.jobstore)

    def modify(self, **changes):
        """Modifies the referenced job."""

        self.scheduler.modify_job(self.id, self.jobstore, **changes)
        self._job_state['id'] = changes.get('id', self.id)
        self.refresh()

    def refresh(self):
        """Reloads the current state of the referenced job from the scheduler."""

        jobhandle = self.scheduler.get_job(self.id, self.jobstore)
        self._job_state = jobhandle._job_state

    @property
    def pending(self):
        """Returns ``True`` if the referenced job is still waiting to be added to its designated job store."""

        for job in self.scheduler.get_jobs(self.jobstore, True):
            if job.id == self.id:
                return True

        return False

    def __getattr__(self, item):
        try:
            return self._job_state[item]
        except KeyError:
            raise AttributeError(item)

    def __setattr__(self, key, value):
        if key in self.__slots__:
            super(JobHandle, self).__setattr__(key, value)
        else:
            self.modify(**{key: value})

    def __eq__(self, other):
        if isinstance(other, JobHandle):
            return self.id == other.id
        return NotImplemented

    def __repr__(self):
        return '<JobHandle (id=%s name=%s)>' % (repr_escape(self.id), repr_escape(self.name))

    def __str__(self):
        return '%s (trigger: %s, next run at: %s)' % (self.name, repr_escape(str(self.trigger)),
                                                      datetime_repr(self.next_run_time))

    def __unicode__(self):
        return six.u('%s (trigger: %s, next run at: %s)') % (self.name, unicode(self.trigger),
                                                             datetime_repr(self.next_run_time))
