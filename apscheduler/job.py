from collections import Iterable, Mapping
from inspect import isfunction, ismethod, getargspec
from threading import Lock
from datetime import timedelta, datetime
from uuid import uuid4

from pkg_resources import iter_entry_points
import six

from apscheduler.util import ref_to_obj, obj_to_ref, datetime_repr, repr_escape, get_callable_name


class MaxInstancesReachedError(Exception):
    pass


class Job(object):
    """
    Encapsulates the actual Job along with its metadata. This class is used internally by APScheduler, and should never
    be instantiated by the user.
    """

    # __slots__ = ('_lock', 'id', 'trigger', 'func', 'func_ref', 'args', 'kwargs', 'name', 'misfire_grace_time',
    #              'coalesce', 'max_runs', 'max_instances', 'runs', 'instances', 'next_run_time')

    trigger_plugins = dict((ep.name, ep) for ep in iter_entry_points('apscheduler.triggers'))
    trigger_classes = {}
    instances = 0
    runs = 0
    next_run_time = None

    def __init__(self, **kwargs):
        super(Job, self).__init__()
        changes = self.validate_changes(kwargs)
        self.modify(changes)
        self._lock = Lock()

    def modify(self, changes):
        for key, value in six.iteritems(changes):
            setattr(self, key, value)

    def validate_changes(self, changes):
        """Validates the changes to the Job and makes the modifications if and only if all of them validate."""

        approved = {}

        if 'id' in changes:
            value = changes.pop('id')
            if value is None:
                value = uuid4().hex
            elif not isinstance(value, six.string_types):
                raise TypeError("id must be a nonempty string")
            approved['id'] = value

        if 'func' in changes or 'args' in changes or 'kwargs' in changes:
            func = changes.pop('func') if 'func' in changes else self.func
            args = changes.pop('args') if 'args' in changes else self.args
            kwargs = changes.pop('kwargs') if 'kwargs' in changes else self.kwargs

            if isinstance(func, six.string_types):
                func_ref = func
                func = ref_to_obj(func)
            elif callable(func):
                try:
                    func_ref = obj_to_ref(func)
                except ValueError:
                    # If this happens, this Job won't be serializable
                    func_ref = None
            else:
                raise TypeError('func must be a callable or a textual reference to one')

            if not hasattr(self, 'name') and changes.get('name', None) is None:
                changes['name'] = get_callable_name(func)

            if isinstance(args, six.string_types) or not isinstance(args, Iterable):
                raise TypeError('args must be a non-string iterable')
            if isinstance(kwargs, six.string_types) or not isinstance(kwargs, Mapping):
                raise TypeError('kwargs must be a dict-like object')
            self.check_callable_args(func, args, kwargs)
            approved['func'] = func
            approved['func_ref'] = func_ref
            approved['args'] = args
            approved['kwargs'] = kwargs

        if 'name' in changes:
            value = changes.pop('name')
            if not value or not isinstance(value, six.string_types):
                raise TypeError("name must be a nonempty string")
            approved['name'] = value

        if 'misfire_grace_time' in changes:
            value = changes.pop('misfire_grace_time')
            if value is not None and (not isinstance(value, six.integer_types) or value <= 0):
                raise TypeError('misfire_grace_time must be either None or a positive integer')
            approved['misfire_grace_time'] = value

        if 'coalesce' in changes:
            value = bool(changes.pop('coalesce'))
            approved['coalesce'] = value

        if 'max_instances' in changes:
            value = changes.pop('max_instances')
            if not isinstance(value, six.integer_types) or value <= 0:
                raise TypeError('max_instances must be a positive integer')
            approved['max_instances'] = value

        if 'runs' in changes or 'max_runs' in changes:
            runs = changes.pop('runs') if 'runs' in changes else self.runs
            max_runs = changes.pop('max_runs') if 'max_runs' in changes else self.max_runs
            if not isinstance(runs, six.integer_types) or runs < 0:
                raise TypeError('runs must be a non-negative integer')
            if max_runs is not None and (not isinstance(max_runs, six.integer_types) or max_runs <= 0):
                raise TypeError('max_runs must be either None or a positive integer')
            if max_runs is not None and runs >= max_runs:
                raise TypeError('runs (%d) may not be equal to or over max_runs (%d)' % (runs, max_runs))
            approved['runs'] = runs
            approved['max_runs'] = max_runs

        if 'trigger' in changes:
            trigger = changes.pop('trigger')
            if isinstance(trigger, str):
                try:
                    trigger_cls = Job.trigger_classes[trigger]
                except KeyError:
                    if trigger in Job.trigger_plugins:
                        trigger_cls = Job.trigger_classes[trigger] = Job.trigger_plugins[trigger].load()
                        if not callable(getattr(trigger_cls, 'get_next_fire_time')):
                            raise TypeError('The trigger entry point does not point to a trigger class')
                    else:
                        raise KeyError('No trigger by the name "%s" was found' % trigger)

                trigger_args = changes.pop('trigger_args')
                if not isinstance(trigger_args, Mapping):
                    raise TypeError('trigger_args must either be a dict-like object or an iterable')

                trigger = trigger_cls(**trigger_args)
            elif not callable(getattr(trigger, 'get_next_fire_time')):
                raise TypeError('Expected a trigger instance, got %s instead' % trigger.__class__.__name__)
            else:
                changes.pop('trigger_args', None)

            approved['trigger'] = trigger

        if 'next_run_time' in changes:
            value = changes.pop('next_run_time')
            if not isinstance(value, datetime):
                raise TypeError('next_run_time must be either None or a datetime instance')
            approved['next_run_time'] = value

        if changes:
            raise AttributeError('The following are not modifiable attributes of Job: %s' % ', '.join(changes))

        return approved

    @staticmethod
    def check_callable_args(func, args, kwargs):
        """Ensures that the given callable can be called with the given arguments."""

        if not isfunction(func) and not ismethod(func) and hasattr(func, '__call__'):
            func = func.__call__
        argspec = getargspec(func)
        argspec_args = argspec.args[1:] if ismethod(func) else argspec.args
        varkw = getattr(argspec, 'varkw', None) or getattr(argspec, 'keywords', None)
        kwargs_set = frozenset(kwargs)
        mandatory_args = frozenset(argspec_args[:-len(argspec.defaults)] if argspec.defaults else argspec_args)
        mandatory_args_matches = frozenset(argspec_args[:len(args)])
        mandatory_kwargs_matches = set(kwargs).intersection(mandatory_args)
        kwonly_args = frozenset(getattr(argspec, 'kwonlyargs', []))
        kwonly_defaults = frozenset(getattr(argspec, 'kwonlydefaults', None) or ())

        # Make sure there are no conflicts between args and kwargs
        pos_kwargs_conflicts = mandatory_args_matches.intersection(mandatory_kwargs_matches)
        if pos_kwargs_conflicts:
            raise ValueError('The following arguments are supplied in both args and kwargs: %s' %
                             ', '.join(pos_kwargs_conflicts))

        # Check that the number of positional arguments minus the number of matched kwargs matches the argspec
        missing_args = mandatory_args - mandatory_args_matches.union(mandatory_kwargs_matches)
        if missing_args:
            raise ValueError('The following arguments are not supplied: %s' % ', '.join(missing_args))

        # Check that the callable can accept the given number of positional arguments
        if not argspec.varargs and len(args) > len(argspec_args):
            raise ValueError('The list of positional arguments is longer than the target callable can handle '
                             '(allowed: %d, given in args: %d)' % (len(argspec_args), len(args)))

        # Check that the callable can accept the given keyword arguments
        if not varkw:
            unmatched_kwargs = kwargs_set - frozenset(argspec_args).union(kwonly_args)
            if unmatched_kwargs:
                raise ValueError('The target callable does not accept the following keyword arguments: %s' %
                                 ', '.join(unmatched_kwargs))

        # Check that all keyword-only arguments have been supplied
        unmatched_kwargs = kwonly_args - kwargs_set - kwonly_defaults
        if unmatched_kwargs:
            raise ValueError('The following keyword-only arguments have not been supplied in kwargs: %s' %
                             ', '.join(unmatched_kwargs))

    def get_run_times(self, now):
        """Computes the scheduled run times between ``next_run_time`` and ``now``."""

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
            'func': self.func_ref,
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
        self.func_ref = state['func']
        self.func = ref_to_obj(self.func_ref)
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
