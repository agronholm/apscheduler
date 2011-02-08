"""
Jobs represent scheduled tasks.
"""

from threading import Lock

from apscheduler.util import to_unicode, ref_to_obj, get_callable_name

__all__ = ('Job', 'STATUS_OK', 'STATUS_ERROR', 'STATUS_MISSED',
           'STATUS_FINISHED', 'STATUS_ALL', 'JobStatus')


class Job(object):
    """
    Encapsulates the actual Job along with its metadata. JobMeta instances
    are created by the scheduler when adding jobs, and it should not be
    directly instantiated.

    :param trigger: trigger that determines the execution times
    :param func: callable to call when the trigger is triggered
    :param args: list of positional arguments to call func with
    :param kwargs: dict of keyword arguments to call func with
    :param name: name of the job (optional)
    :param misfire_grace_time: seconds after the designated run time that
        the job is still allowed to be run
    :param max_runs: maximum number of times this job is allowed to be
        triggered
    :param max_running_instances: maximum number of concurrently running
        instances of this job
    """

    id = None
    next_run_time = None

    def __init__(self, trigger, func, args, kwargs, misfire_grace_time,
                 name=None, max_runs=None, max_concurrency=1):
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
        if max_concurrency <= 0:
            raise ValueError('max_concurrency must be a positive value')

        self._lock = Lock()

        self.trigger = trigger
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.name = to_unicode(name or get_callable_name(func))
        self.misfire_grace_time = misfire_grace_time
        self.max_runs = max_runs
        self.max_concurrency = max_concurrency
        self.runs = 0
        self.instances = 0

    def compute_next_run_time(self, now):
        if self.runs == self.max_runs:
            self.next_run_time = None
        else:
            self.next_run_time = self.trigger.get_next_fire_time(now)

    def add_instance(self):
        self._lock.acquire()
        self.instances += 1
        self._lock.release()

    def remove_instance(self):
        if self.instances == 0:
            raise ValueError('Already at 0 instances')
        self._lock.acquire()
        self.instances -= 1
        self._lock.release()

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
        return '<Job (name=%s, trigger=%s)>' % (self.name, repr(self.trigger))

    def __str__(self):
        return '%s (trigger: %s, next run at: %s)' % (self.name,
            str(self.trigger), str(self.next_run_time))


STATUS_OK = 1
STATUS_ERROR = 2
STATUS_MISSED = 4
STATUS_FINISHED = 8
STATUS_ALL = STATUS_OK | STATUS_ERROR | STATUS_MISSED | STATUS_FINISHED

class JobStatus(object):
    code = None
    retval = None
    exception = None
    traceback = None

    def __init__(self, job, scheduled_run_time):
        self.job = job
        self.scheduled_run_time = scheduled_run_time
