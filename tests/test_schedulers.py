from logging import StreamHandler, getLogger, INFO
from datetime import datetime, timedelta
from threading import Thread

from pytz import utc
import pytest
import six

from apscheduler.executors.base import BaseExecutor, MaxInstancesReachedError
from apscheduler.executors.debug import DebugExecutor
from apscheduler.job import Job
from apscheduler.jobstores.base import BaseJobStore, JobLookupError, ConflictingIdError
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.schedulers import SchedulerAlreadyRunningError, SchedulerNotRunningError
from apscheduler.schedulers.base import BaseScheduler
from apscheduler.events import (EVENT_SCHEDULER_START, EVENT_SCHEDULER_SHUTDOWN, EVENT_JOBSTORE_ADDED,
                                EVENT_JOBSTORE_REMOVED, EVENT_ALL, EVENT_ALL_JOBS_REMOVED, EVENT_EXECUTOR_ADDED,
                                EVENT_EXECUTOR_REMOVED, EVENT_JOB_MODIFIED, EVENT_JOB_REMOVED, SchedulerEvent,
                                EVENT_JOB_ADDED, EVENT_JOB_EXECUTED)
from apscheduler.triggers.base import BaseTrigger
from apscheduler.util import undefined


try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

try:
    from unittest.mock import MagicMock, patch
except ImportError:
    from mock import MagicMock, patch


class OrderedDict(object):
    def __init__(self, items):
        self._items = items
        self._items_dict = dict(items)

    def items(self):
        return iter(self._items)

    def __getitem__(self, key):
        return self._items_dict[key]

    iteritems = items


class DummyScheduler(BaseScheduler):
    def start(self):
        super(DummyScheduler, self).start()

    def shutdown(self, wait=True):
        super(DummyScheduler, self).shutdown(wait)

    def wakeup(self):
        pass


class DummyTrigger(BaseTrigger):
    def __init__(self, **args):
        self.args = args

    def get_next_fire_time(self, previous_fire_time, now):
        pass


class DummyExecutor(BaseExecutor):
    def __init__(self, **args):
        super(DummyExecutor, self).__init__()
        self.args = args

    def _do_submit_job(self, job, run_times):
        pass


class DummyJobStore(BaseJobStore):
    def __init__(self, **args):
        super(DummyJobStore, self).__init__()
        self.args = args

    def get_due_jobs(self, now):
        pass

    def lookup_job(self, job_id):
        pass

    def remove_job(self, job_id):
        pass

    def remove_all_jobs(self):
        pass

    def get_next_run_time(self):
        pass

    def get_all_jobs(self):
        pass

    def add_job(self, job):
        pass

    def update_job(self, job):
        pass


@pytest.fixture
def scheduler(monkeypatch, timezone):
    monkeypatch.setattr('apscheduler.schedulers.base.get_localzone', MagicMock(return_value=timezone))
    return DummyScheduler()


@pytest.fixture
def logstream(request):
    stream = StringIO()
    loghandler = StreamHandler(stream)
    loghandler.setLevel(INFO)
    logger = getLogger('apscheduler')
    logger.addHandler(loghandler)
    request.addfinalizer(lambda: logger.removeHandler(loghandler))
    return stream


class TestBaseScheduler(object):
    def test_constructor(self):
        with patch('%s.DummyScheduler.configure' % __name__) as configure:
            gconfig = {'apscheduler.foo': 'bar', 'apscheduler.x': 'y'}
            options = {'bar': 'baz', 'xyz': 123}
            DummyScheduler(gconfig, **options)

        configure.assert_called_once_with(gconfig, **options)

    @pytest.mark.parametrize('gconfig', [
        {
            'apscheduler.timezone': 'UTC',
            'apscheduler.job_defaults.misfire_grace_time': '5',
            'apscheduler.job_defaults.coalesce': 'false',
            'apscheduler.job_defaults.max_instances': '9',
            'apscheduler.executors.default.class': '%s:DummyExecutor' % __name__,
            'apscheduler.executors.default.arg1': '3',
            'apscheduler.executors.default.arg2': 'a',
            'apscheduler.executors.alter.class': '%s:DummyExecutor' % __name__,
            'apscheduler.executors.alter.arg': 'true',
            'apscheduler.jobstores.default.class': '%s:DummyJobStore' % __name__,
            'apscheduler.jobstores.default.arg1': '3',
            'apscheduler.jobstores.default.arg2': 'a',
            'apscheduler.jobstores.bar.class': '%s:DummyJobStore' % __name__,
            'apscheduler.jobstores.bar.arg': 'false',
        },
        {
            'apscheduler.timezone': 'UTC',
            'apscheduler.job_defaults': {
                'misfire_grace_time': '5',
                'coalesce': 'false',
                'max_instances': '9',
            },
            'apscheduler.executors': {
                'default': {'class': '%s:DummyExecutor' % __name__, 'arg1': '3', 'arg2': 'a'},
                'alter': {'class': '%s:DummyExecutor' % __name__, 'arg': 'true'}
            },
            'apscheduler.jobstores': {
                'default': {'class': '%s:DummyJobStore' % __name__, 'arg1': '3', 'arg2': 'a'},
                'bar': {'class': '%s:DummyJobStore' % __name__, 'arg': 'false'}
            }
        }
    ], ids=['ini-style', 'yaml-style'])
    def test_configure(self, scheduler, gconfig):
        scheduler._configure = MagicMock()
        scheduler.configure(gconfig, timezone='Other timezone')

        scheduler._configure.assert_called_once_with({
            'timezone': 'Other timezone',
            'job_defaults': {
                'misfire_grace_time': '5',
                'coalesce': 'false',
                'max_instances': '9',
            },
            'executors': {
                'default': {'class': '%s:DummyExecutor' % __name__, 'arg1': '3', 'arg2': 'a'},
                'alter': {'class': '%s:DummyExecutor' % __name__, 'arg': 'true'}
            },
            'jobstores': {
                'default': {'class': '%s:DummyJobStore' % __name__, 'arg1': '3', 'arg2': 'a'},
                'bar': {'class': '%s:DummyJobStore' % __name__, 'arg': 'false'}
            }
        })

    def test_configure_already_running(self, scheduler):
        scheduler._stopped = False
        pytest.raises(SchedulerAlreadyRunningError, scheduler.configure, {})

    def test_start(self, scheduler, create_job):
        scheduler._executors = {'exec1': MagicMock(BaseExecutor), 'exec2': MagicMock(BaseExecutor)}
        scheduler._jobstores = {'store1': MagicMock(BaseJobStore), 'store2': MagicMock(BaseJobStore)}
        job = create_job(func=lambda: None)
        scheduler._pending_jobs = [(job, 'store1', False)]
        scheduler._real_add_job = MagicMock()
        scheduler._dispatch_event = MagicMock()
        scheduler.start()

        scheduler._executors['exec1'].start.assert_called_once_with(scheduler, 'exec1')
        scheduler._executors['exec2'].start.assert_called_once_with(scheduler, 'exec2')
        scheduler._jobstores['store1'].start.assert_called_once_with(scheduler, 'store1')
        scheduler._jobstores['store2'].start.assert_called_once_with(scheduler, 'store2')
        assert len(scheduler._executors) == 3
        assert len(scheduler._jobstores) == 3
        assert 'default' in scheduler._executors
        assert 'default' in scheduler._jobstores

        scheduler._real_add_job.assert_called_once_with(job, 'store1', False, False)
        assert scheduler._pending_jobs == []

        assert scheduler._dispatch_event.call_count == 3
        event = scheduler._dispatch_event.call_args_list[0][0][0]
        assert event.code == EVENT_EXECUTOR_ADDED
        assert event.alias == 'default'
        event = scheduler._dispatch_event.call_args_list[1][0][0]
        assert event.code == EVENT_JOBSTORE_ADDED
        assert event.alias == 'default'
        event = scheduler._dispatch_event.call_args_list[2][0][0]
        assert event.code == EVENT_SCHEDULER_START

        assert not scheduler._stopped

    def test_start_already_running(self, scheduler):
        scheduler._stopped = False
        pytest.raises(SchedulerAlreadyRunningError, scheduler.start)

    @pytest.mark.parametrize('wait', [True, False], ids=['wait', 'nowait'])
    def test_shutdown(self, scheduler, wait):
        scheduler._executors = {'exec1': MagicMock(BaseExecutor), 'exec2': MagicMock(BaseExecutor)}
        scheduler._jobstores = {'store1': MagicMock(BaseJobStore), 'store2': MagicMock(BaseJobStore)}
        scheduler._stopped = False
        scheduler._dispatch_event = MagicMock()
        scheduler.shutdown(wait)

        assert scheduler._stopped is True
        assert scheduler._dispatch_event.call_count == 1
        event = scheduler._dispatch_event.call_args[0][0]
        assert event.code == EVENT_SCHEDULER_SHUTDOWN

        for executor in scheduler._executors.values():
            executor.shutdown.assert_called_once_with(wait)
        for jobstore in scheduler._jobstores.values():
            jobstore.shutdown.assert_called_once_with()

    def test_shutdown_not_running(self, scheduler):
        pytest.raises(SchedulerNotRunningError, scheduler.shutdown)

    @pytest.mark.parametrize('stopped', [True, False], ids=['stopped=True', 'stopped=False'])
    def test_running(self, scheduler, stopped):
        scheduler._stopped = stopped
        assert scheduler.running is not stopped

    @pytest.mark.parametrize('stopped', [True, False], ids=['stopped=True', 'stopped=False'])
    def test_add_executor(self, scheduler, stopped):
        scheduler._stopped = stopped
        executor = DebugExecutor()
        executor.start = MagicMock()
        scheduler.add_executor(executor)

        assert scheduler._executors == {'default': executor}
        if not stopped:
            executor.start.assert_called_once_with(scheduler)
        else:
            assert executor.start.call_count == 0

    def test_add_executor_already_exists(self, scheduler):
        executor = DebugExecutor()
        scheduler.add_executor(executor)
        exc = pytest.raises(ValueError, scheduler.add_executor, executor)
        assert str(exc.value) == 'This scheduler already has an executor by the alias of "default"'

    def test_remove_executor(self, scheduler):
        scheduler.add_executor(DebugExecutor(), 'foo')
        scheduler._dispatch_event = MagicMock()
        scheduler.remove_executor('foo')

        assert scheduler._executors == {}
        assert scheduler._dispatch_event.call_count == 1
        event = scheduler._dispatch_event.call_args[0][0]
        assert event.code == EVENT_EXECUTOR_REMOVED
        assert event.alias == 'foo'

    def test_remove_executor_nonexistent(self, scheduler):
        pytest.raises(KeyError, scheduler.remove_executor, 'foo')

    @pytest.mark.parametrize('stopped', [True, False], ids=['stopped=True', 'stopped=False'])
    def test_add_jobstore(self, scheduler, stopped):
        scheduler._stopped = stopped
        jobstore = MemoryJobStore()
        jobstore.start = MagicMock()
        scheduler._real_add_job = MagicMock()
        scheduler._dispatch_event = MagicMock()
        scheduler.wakeup = MagicMock()
        scheduler.add_jobstore(jobstore)

        assert scheduler._dispatch_event.call_count == 1
        event = scheduler._dispatch_event.call_args[0][0]
        assert event.code == EVENT_JOBSTORE_ADDED
        assert event.alias == 'default'
        if stopped:
            assert jobstore.start.call_count == 0
            assert scheduler.wakeup.call_count == 0
        else:
            scheduler.wakeup.assert_called_once_with()
            jobstore.start.assert_called_once_with(scheduler, 'default')

    def test_add_jobstore_already_exists(self, scheduler):
        jobstore = MemoryJobStore()
        scheduler.add_jobstore(jobstore)
        exc = pytest.raises(ValueError, scheduler.add_jobstore, jobstore)
        assert str(exc.value) == 'This scheduler already has a job store by the alias of "default"'

    def test_remove_jobstore(self, scheduler):
        scheduler.add_jobstore(MemoryJobStore(), 'foo')
        scheduler._dispatch_event = MagicMock()
        scheduler.remove_jobstore('foo')

        assert scheduler._jobstores == {}
        assert scheduler._dispatch_event.call_count == 1
        event = scheduler._dispatch_event.call_args[0][0]
        assert event.code == EVENT_JOBSTORE_REMOVED
        assert event.alias == 'foo'

    def test_remove_jobstore_nonexistent(self, scheduler):
        pytest.raises(KeyError, scheduler.remove_jobstore, 'foo')

    @pytest.mark.parametrize('event', [EVENT_ALL, EVENT_JOBSTORE_ADDED], ids=['default', 'explicit value'])
    def test_add_listener(self, scheduler, event):
        listener = lambda event: None
        args = (event,) if event != EVENT_ALL else ()
        scheduler.add_listener(listener, *args)
        assert scheduler._listeners == [(listener, event)]

    def test_remove_listener(self, scheduler):
        func = lambda: None
        func2 = lambda: None
        scheduler._listeners = [(func, EVENT_ALL), (func2, EVENT_JOBSTORE_ADDED)]
        scheduler.remove_listener(func)
        assert scheduler._listeners == [(func2, EVENT_JOBSTORE_ADDED)]

    @pytest.mark.parametrize('stopped', [True, False], ids=['stopped=True', 'stopped=False'])
    def test_add_job(self, scheduler, stopped, timezone):
        func = lambda x, y: None
        scheduler._stopped = stopped
        scheduler._real_add_job = MagicMock()
        job = scheduler.add_job(func, 'date', [1], {'y': 2}, 'my-id', 'dummy',
                                next_run_time=datetime(2014, 5, 23, 10),
                                run_date='2014-06-01 08:41:00')

        assert isinstance(job, Job)
        assert job.id == 'my-id'
        assert not hasattr(job, 'misfire_grace_time')
        assert not hasattr(job, 'coalesce')
        assert not hasattr(job, 'max_instances')
        assert job.next_run_time.tzinfo.zone == timezone.zone
        assert len(scheduler._pending_jobs) == (1 if stopped else 0)
        assert scheduler._real_add_job.call_count == (0 if stopped else 1)

    def test_scheduled_job(self, scheduler):
        func = lambda x, y: None
        scheduler.add_job = MagicMock()
        decorator = scheduler.scheduled_job('date', [1], {'y': 2}, 'my-id', 'dummy', run_date='2014-06-01 08:41:00')
        decorator(func)

        scheduler.add_job.assert_called_once_with(func, 'date', [1], {'y': 2}, 'my-id', 'dummy', undefined, undefined,
                                                  undefined, undefined, 'default', 'default', True,
                                                  run_date='2014-06-01 08:41:00')

    @pytest.mark.parametrize('pending', [True, False], ids=['pending job', 'scheduled job'])
    def test_modify_job(self, scheduler, pending, timezone):
        job = MagicMock()
        scheduler._dispatch_event = MagicMock()
        scheduler._lookup_job = MagicMock(return_value=(job, None if pending else 'default'))
        if not pending:
            jobstore = MagicMock()
            scheduler._lookup_jobstore = lambda alias: jobstore if alias == 'default' else None
        scheduler.modify_job('blah', misfire_grace_time=5, max_instances=2, next_run_time=datetime(2014, 10, 17))

        job._modify.assert_called_once_with(misfire_grace_time=5, max_instances=2, next_run_time=datetime(2014, 10, 17))
        if not pending:
            jobstore.update_job.assert_called_once_with(job)

        assert scheduler._dispatch_event.call_count == 1
        event = scheduler._dispatch_event.call_args[0][0]
        assert event.code == EVENT_JOB_MODIFIED
        assert event.jobstore == (None if pending else 'default')

    def test_reschedule_job(self, scheduler):
        scheduler.modify_job = MagicMock()
        trigger = MagicMock(get_next_fire_time=lambda previous, now: 1)
        scheduler._create_trigger = MagicMock(return_value=trigger)
        scheduler.reschedule_job('my-id', 'jobstore', 'date', run_date='2014-06-01 08:41:00')

        assert scheduler.modify_job.call_count == 1
        assert scheduler.modify_job.call_args[0] == ('my-id', 'jobstore')
        assert scheduler.modify_job.call_args[1] == {'trigger': trigger, 'next_run_time': 1}

    def test_pause_job(self, scheduler):
        scheduler.modify_job = MagicMock()
        scheduler.pause_job('job_id', 'jobstore')

        scheduler.modify_job.assert_called_once_with('job_id', 'jobstore', next_run_time=None)

    @pytest.mark.parametrize('dead_job', [True, False], ids=['dead job', 'live job'])
    def test_resume_job(self, scheduler, freeze_time, dead_job):
        next_fire_time = None if dead_job else freeze_time.current + timedelta(seconds=1)
        trigger = MagicMock(BaseTrigger, get_next_fire_time=lambda prev, now: next_fire_time)
        returned_job = MagicMock(Job, id='foo', trigger=trigger)
        scheduler._lookup_job = MagicMock(return_value=(returned_job, 'bar'))
        scheduler.modify_job = MagicMock()
        scheduler.remove_job = MagicMock()
        scheduler.resume_job('foo')

        if dead_job:
            scheduler.remove_job.assert_called_once_with('foo', 'bar')
        else:
            scheduler.modify_job.assert_called_once_with('foo', 'bar', next_run_time=next_fire_time)

    @pytest.mark.parametrize('pending', [True, False, None], ids=['pending only', 'no pending', 'both'])
    @pytest.mark.parametrize('jobstore', [None, 'baz'], ids=['all jobstores', 'specific jobstore'])
    def test_get_jobs(self, scheduler, pending, jobstore):
        pending_job1 = object()
        pending_job2 = object()
        scheduled_job1 = object()
        scheduled_job2 = object()
        scheduler._pending_jobs = [(pending_job1, 'bar', False), (pending_job2, 'baz', False)]
        scheduler._jobstores = OrderedDict([
            ('bar', MagicMock(BaseJobStore, get_all_jobs=lambda: [scheduled_job1])),
            ('baz', MagicMock(BaseJobStore, get_all_jobs=lambda: [scheduled_job2])),
        ])
        jobs = scheduler.get_jobs(jobstore, pending)

        if pending is True:
            assert jobs == ([pending_job1, pending_job2] if jobstore is None else [pending_job2])
        elif pending is False:
            assert jobs == ([scheduled_job1, scheduled_job2] if jobstore is None else [scheduled_job2])
        else:
            assert jobs == ([pending_job1, pending_job2, scheduled_job1, scheduled_job2] if jobstore is None else
                            [pending_job2, scheduled_job2])

    @pytest.mark.parametrize('jobstore', [None, 'bar'], ids=['any jobstore', 'specific jobstore'])
    def test_get_job(self, scheduler, jobstore):
        returned_job = object()
        scheduler._lookup_job = MagicMock(return_value=(returned_job, 'bar'))
        job = scheduler.get_job('foo', jobstore)

        assert job is returned_job

    def test_get_job_nonexistent_job(self, scheduler):
        scheduler._lookup_job = MagicMock(side_effect=JobLookupError('foo'))
        assert scheduler.get_job('foo') is None

    def test_get_job_nonexistent_jobstore(self, scheduler):
        assert scheduler.get_job('foo', 'bar') is None

    @pytest.mark.parametrize('pending', [True, False], ids=['pending', 'nor pending'])
    @pytest.mark.parametrize('jobstore', [None, 'bar'], ids=['any jobstore', 'specific jobstore'])
    def test_remove_job(self, scheduler, pending, jobstore):
        if pending:
            scheduler._pending_jobs.append((MagicMock(Job, id='foo'), 'bar', False))
        scheduler._dispatch_event = MagicMock()
        scheduler._jobstores = OrderedDict([
            ('baz', MagicMock(BaseJobStore, remove_job=MagicMock(side_effect=JobLookupError('foo')))),
            ('bar', MagicMock(BaseJobStore)),
        ])
        scheduler.remove_job('foo', jobstore)

        if pending:
            assert scheduler._jobstores['bar'].remove_job.call_count == 0
        else:
            scheduler._jobstores['bar'].remove_job.assert_called_once_with('foo')

        assert len(scheduler._pending_jobs) == 0
        assert scheduler._dispatch_event.call_count == 1
        event = scheduler._dispatch_event.call_args[0][0]
        assert event.code == EVENT_JOB_REMOVED

    def test_remove_nonexistent_job(self, scheduler):
        pytest.raises(JobLookupError, scheduler.remove_job, 'foo')

    @pytest.mark.parametrize('jobstore', [None, 'alter', 'third'], ids=['all', 'single jobstore', 'nonexistent store'])
    def test_remove_all_jobs(self, scheduler, jobstore):
        """Tests that remove_all_jobs() removes all jobs from all attached job stores, plus any pending jobs."""

        scheduler._jobstores = OrderedDict([
            ('default', MagicMock(BaseJobStore)),
            ('alter', MagicMock(BaseJobStore))
        ])
        scheduler._pending_jobs = [
            (MagicMock(Job), 'default', False),
            (MagicMock(Job), 'alter', False),
            (MagicMock(Job), 'third', False)
        ]
        scheduler._dispatch_event = MagicMock()
        scheduler.remove_all_jobs(jobstore)

        if jobstore == 'alter':
            scheduler._jobstores['alter'].remove_all_jobs.assert_called_once_with()
            assert scheduler._jobstores['default'].remove_all_jobs.call_count == 0
            assert len(scheduler._pending_jobs) == 2
        elif jobstore == 'third':
            assert scheduler._jobstores['alter'].remove_all_jobs.call_count == 0
            assert scheduler._jobstores['default'].remove_all_jobs.call_count == 0
            assert len(scheduler._pending_jobs) == 2
        else:
            scheduler._jobstores['alter'].remove_all_jobs.assert_called_once_with()
            scheduler._jobstores['default'].remove_all_jobs.assert_called_once_with()
            assert len(scheduler._pending_jobs) == 0

        assert scheduler._dispatch_event.call_count == 1
        event = scheduler._dispatch_event.call_args[0][0]
        assert event.code == EVENT_ALL_JOBS_REMOVED
        assert event.alias == jobstore

    @pytest.mark.parametrize('jobstore', [None, 'bar'], ids=['all jobstores', 'specific jobstore'])
    def test_print_jobs(self, scheduler, jobstore):
        outfile = StringIO()
        bar_job = MagicMock(Job)
        bar_job.__str__ = bar_job.__unicode__ = MagicMock(return_value=six.u('bar job 1'))
        pending_bar_job = MagicMock(Job)
        pending_bar_job.__str__ = pending_bar_job.__unicode__ = MagicMock(return_value=six.u('pending bar job 1'))
        pending_baz_job = MagicMock(Job)
        pending_baz_job.__str__ = pending_baz_job.__unicode__ = MagicMock(return_value=six.u('pending baz job 1'))
        scheduler._jobstores = OrderedDict([
            ('bar', MagicMock(BaseJobStore, get_all_jobs=lambda: [bar_job])),
            ('baz', MagicMock(BaseJobStore, get_all_jobs=lambda: []))
        ])
        scheduler._pending_jobs = [
            (pending_bar_job, 'bar', False),
            (pending_baz_job, 'baz', False),
        ]
        scheduler.print_jobs(jobstore, outfile)

        if jobstore:
            assert outfile.getvalue() == """\
Pending jobs:
    pending bar job 1
Jobstore bar:
    bar job 1
"""
        else:
            assert outfile.getvalue() == """\
Pending jobs:
    pending bar job 1
    pending baz job 1
Jobstore bar:
    bar job 1
Jobstore baz:
    No scheduled jobs
"""

    @pytest.mark.parametrize('config', [
        {
            'timezone': 'UTC',
            'job_defaults': {
                'misfire_grace_time': '5',
                'coalesce': 'false',
                'max_instances': '9',
            },
            'executors': {
                'default': {'class': '%s:DummyExecutor' % __name__, 'arg1': '3', 'arg2': 'a'},
                'alter': {'class': '%s:DummyExecutor' % __name__, 'arg': 'true'}
            },
            'jobstores': {
                'default': {'class': '%s:DummyJobStore' % __name__, 'arg1': '3', 'arg2': 'a'},
                'bar': {'class': '%s:DummyJobStore' % __name__, 'arg': 'false'}
            }
        },
        {
            'timezone': utc,
            'job_defaults': {
                'misfire_grace_time': 5,
                'coalesce': False,
                'max_instances': 9,
            },
            'executors': {
                'default': DummyExecutor(arg1='3', arg2='a'),
                'alter': DummyExecutor(arg='true')
            },
            'jobstores': {
                'default': DummyJobStore(arg1='3', arg2='a'),
                'bar': DummyJobStore(arg='false')
            }
        }
    ], ids=['references', 'instances'])
    def test_configure_private(self, scheduler, config):
        scheduler._configure(config)

        assert scheduler.timezone is utc
        assert scheduler._job_defaults == {
            'misfire_grace_time': 5,
            'coalesce': False,
            'max_instances': 9
        }
        assert set(six.iterkeys(scheduler._executors)) == set(['default', 'alter'])
        assert scheduler._executors['default'].args == {'arg1': '3', 'arg2': 'a'}
        assert scheduler._executors['alter'].args == {'arg': 'true'}
        assert set(six.iterkeys(scheduler._jobstores)) == set(['default', 'bar'])
        assert scheduler._jobstores['default'].args == {'arg1': '3', 'arg2': 'a'}
        assert scheduler._jobstores['bar'].args == {'arg': 'false'}

    def test_configure_private_invalid_executor(self, scheduler):
        exc = pytest.raises(TypeError, scheduler._configure, {'executors': {'default': 6}})
        assert str(exc.value) == "Expected executor instance or dict for executors['default'], got int instead"

    def test_configure_private_invalid_jobstore(self, scheduler):
        exc = pytest.raises(TypeError, scheduler._configure, {'jobstores': {'default': 6}})
        assert str(exc.value) == "Expected job store instance or dict for jobstores['default'], got int instead"

    def test_create_default_executor(self, scheduler):
        executor = scheduler._create_default_executor()
        assert isinstance(executor, BaseExecutor)

    def test_create_default_jobstore(self, scheduler):
        store = scheduler._create_default_jobstore()
        assert isinstance(store, BaseJobStore)

    def test_lookup_executor(self, scheduler):
        executor = object()
        scheduler._executors = {'executor': executor}
        assert scheduler._lookup_executor('executor') is executor

    def test_lookup_executor_nonexistent(self, scheduler):
        pytest.raises(KeyError, scheduler._lookup_executor, 'executor')

    def test_lookup_jobstore(self, scheduler):
        store = object()
        scheduler._jobstores = {'store': store}
        assert scheduler._lookup_jobstore('store') is store

    def test_lookup_jobstore_nonexistent(self, scheduler):
        pytest.raises(KeyError, scheduler._lookup_jobstore, 'store')

    @pytest.mark.parametrize('pending', [True, False], ids=['pending', 'nor pending'])
    @pytest.mark.parametrize('jobstore', [None, 'baz'], ids=['all jobstores', 'specific jobstore'])
    def test_lookup_job(self, scheduler, pending, jobstore):
        job = MagicMock(Job, id='foo')
        scheduler._jobstores = OrderedDict([
            ('bar', MagicMock(BaseJobStore, lookup_job=lambda job_id: None)),
            ('baz', MagicMock(BaseJobStore, lookup_job=lambda job_id: None))
        ])
        if pending:
            scheduler._pending_jobs = [(job, 'baz', False)]
        else:
            scheduler._jobstores['baz'].lookup_job = MagicMock(return_value=job)

        assert scheduler._lookup_job('foo', jobstore) == (job, None if pending else 'baz')

    @pytest.mark.parametrize('jobstore', [None, 'bar'], ids=['all jobstores', 'specific jobstore'])
    def test_lookup_job_nonexistent_job(self, scheduler, jobstore):
        scheduler._jobstores = OrderedDict([
            ('bar', MagicMock(BaseJobStore, lookup_job=lambda job_id: None)),
            ('baz', MagicMock(BaseJobStore, lookup_job=lambda job_id: None))
        ])
        pytest.raises(JobLookupError, scheduler._lookup_job, 'foo', jobstore)

    def test_dispatch_event(self, scheduler):
        event = SchedulerEvent(1)
        scheduler._listeners = [(MagicMock(), 2), (MagicMock(side_effect=Exception), 1), (MagicMock(), 1)]
        scheduler._dispatch_event(event)

        assert not scheduler._listeners[0][0].called
        scheduler._listeners[1][0].assert_called_once_with(event)

    @pytest.mark.parametrize('job_exists', [True, False], ids=['job exists', 'new job'])
    @pytest.mark.parametrize('replace_existing', [True, False], ids=['replace', 'no replace'])
    @pytest.mark.parametrize('wakeup', [True, False], ids=['wakeup', 'no wakeup'])
    def test_real_add_job(self, scheduler, job_exists, replace_existing, wakeup):
        job = Job(scheduler, id='foo', func=lambda: None, args=(), kwargs={}, next_run_time=None)
        jobstore = MagicMock(BaseJobStore, _alias='bar',
                             add_job=MagicMock(side_effect=ConflictingIdError('foo') if job_exists else None))
        scheduler.wakeup = MagicMock()
        scheduler._job_defaults = {'misfire_grace_time': 3, 'coalesce': False, 'max_instances': 6}
        scheduler._dispatch_event = MagicMock()
        scheduler._jobstores = {'bar': jobstore}

        # Expect and exception if the job already exists and we're not trying to replace it
        if job_exists and not replace_existing:
            pytest.raises(ConflictingIdError, scheduler._real_add_job, job, 'bar', replace_existing, wakeup)
            return

        scheduler._real_add_job(job, 'bar', replace_existing, wakeup)

        # Check that the undefined values were replaced with scheduler defaults
        assert job.misfire_grace_time == 3
        assert job.coalesce is False
        assert job.max_instances == 6
        assert job.next_run_time is None

        if job_exists:
            jobstore.update_job.assert_called_once_with(job)
        else:
            assert not jobstore.update_job.called

        if wakeup:
            scheduler.wakeup.assert_called_once_with()
        else:
            assert not scheduler.wakeup.called

        assert job._jobstore_alias == 'bar'

        assert scheduler._dispatch_event.call_count == 1
        event = scheduler._dispatch_event.call_args[0][0]
        assert event.code == EVENT_JOB_ADDED
        assert event.job_id == 'foo'

    @pytest.mark.parametrize('load_plugin', [True, False], ids=['load plugin', 'plugin loaded'])
    def test_create_trigger(self, scheduler, load_plugin):
        """Tests that creating a trigger with an already loaded plugin works."""

        scheduler._trigger_plugins = {}
        scheduler._trigger_classes = {}
        if load_plugin:
            scheduler._trigger_plugins['dummy'] = MagicMock(load=MagicMock(return_value=DummyTrigger))
        else:
            scheduler._trigger_classes['dummy'] = DummyTrigger

        result = scheduler._create_trigger('dummy', {'a': 1, 'b': 'x'})

        assert isinstance(result, DummyTrigger)
        assert result.args == {'a': 1, 'b': 'x', 'timezone': scheduler.timezone}

    def test_create_trigger_instance(self, scheduler):
        """Tests that passing a trigger instance will return the instance as-is."""

        trigger_instance = DummyTrigger()

        assert scheduler._create_trigger(trigger_instance, {}) is trigger_instance

    def test_create_trigger_default_type(self, scheduler):
        """Tests that passing None as the trigger will create a "date" trigger instance."""

        scheduler._trigger_classes = {'date': DummyTrigger}
        result = scheduler._create_trigger(None, {'a': 1})

        assert isinstance(result, DummyTrigger)
        assert result.args == {'a': 1, 'timezone': scheduler.timezone}

    def test_create_trigger_bad_trigger_type(self, scheduler):
        exc = pytest.raises(TypeError, scheduler._create_trigger, 1, {})
        assert str(exc.value) == 'Expected a trigger instance or string, got int instead'

    def test_create_trigger_bad_plugin_type(self, scheduler):
        scheduler._trigger_classes = {}
        scheduler._trigger_plugins = {'dummy': MagicMock(return_value=object)}
        exc = pytest.raises(TypeError, scheduler._create_trigger, 'dummy', {})
        assert str(exc.value) == 'The trigger entry point does not point to a trigger class'

    def test_create_trigger_nonexisting_plugin(self, scheduler):
        exc = pytest.raises(LookupError, scheduler._create_trigger, 'dummy', {})
        assert str(exc.value) == 'No trigger by the name "dummy" was found'

    def test_create_lock(self, scheduler):
        lock = scheduler._create_lock()
        assert hasattr(lock, '__enter__')

    def test_process_jobs_empty(self, scheduler):
        assert scheduler._process_jobs() is None


class TestProcessJobs(object):
    @pytest.fixture
    def job(self):
        job = MagicMock(Job, id=999, executor='default')
        job.trigger = MagicMock(get_next_fire_time=MagicMock(return_value=None))
        job. __str__ = lambda x: 'job 999'
        return job

    @pytest.fixture
    def jobstore(self, scheduler, job):
        jobstore = MagicMock(BaseJobStore, get_due_jobs=MagicMock(return_value=[job]),
                             get_next_run_time=MagicMock(return_value=None))
        scheduler._jobstores['default'] = jobstore
        return jobstore

    @pytest.fixture
    def executor(self, scheduler):
        executor = MagicMock(BaseExecutor)
        scheduler._executors['default'] = executor
        return executor

    def test_nonexistent_executor(self, scheduler, jobstore, logstream):
        """Tests that an error is logged and the job is removed from its job store if its executor is not found."""

        assert scheduler._process_jobs() is None
        jobstore.remove_job.assert_called_once_with(999)
        assert logstream.getvalue() == \
            'Executor lookup ("default") failed for job "job 999" -- removing it from the job store\n'

    def test_max_instances_reached(self, scheduler, job, jobstore, executor, logstream):
        """Tests that a warning is logged when the maximum instances of a job is reached."""

        executor.submit_job = MagicMock(side_effect=MaxInstancesReachedError(job))

        assert scheduler._process_jobs() is None
        assert logstream.getvalue() == \
            'Execution of job "job 999" skipped: maximum number of running instances reached (1)\n'

    def test_executor_error(self, scheduler, job, jobstore, executor, logstream):
        """Tests that if any exception is raised in executor.submit(), it is logged."""

        executor.submit_job = MagicMock(side_effect=Exception('test message'))

        assert scheduler._process_jobs() is None
        assert 'test message' in logstream.getvalue()
        assert 'Error submitting job "job 999" to executor "default"' in logstream.getvalue()

    def test_job_update(self, scheduler, job, jobstore, executor, logstream, freeze_time):
        """Tests that the job is updated in its job store with the next run time from the trigger."""

        next_run_time = freeze_time.current + timedelta(seconds=6)
        job.trigger.get_next_fire_time = MagicMock(return_value=next_run_time)
        assert scheduler._process_jobs() is None
        job._modify.assert_called_once_with(next_run_time=next_run_time)
        jobstore.update_job.assert_called_once_with(job)

    def test_wait_time(self, scheduler, job, executor, freeze_time):
        """Tests that the earliest next run time from all job stores is returned (ignoring Nones)."""

        scheduler._jobstores = {
            'default': MagicMock(get_next_run_time=MagicMock(return_value=freeze_time.current + timedelta(seconds=8))),
            'alter': MagicMock(get_next_run_time=MagicMock(return_value=None)),
            'another': MagicMock(get_next_run_time=MagicMock(return_value=freeze_time.current + timedelta(seconds=5))),
            'more': MagicMock(get_next_run_time=MagicMock(return_value=freeze_time.current + timedelta(seconds=6))),
        }

        assert scheduler._process_jobs() == 5


class SchedulerImplementationTestBase(object):
    @pytest.fixture(autouse=True)
    def executor(self, scheduler):
        scheduler.add_executor(DebugExecutor())

    @pytest.fixture
    def start_scheduler(self, request, scheduler):
        def cleanup():
            if scheduler.running:
                scheduler.shutdown()

        request.addfinalizer(cleanup)
        return scheduler.start

    @pytest.fixture
    def eventqueue(self, scheduler):
        from six.moves.queue import Queue
        events = Queue()
        scheduler.add_listener(events.put)
        return events

    def wait_event(self, queue):
        return queue.get(True, 1)

    def test_add_pending_job(self, scheduler, freeze_time, eventqueue, start_scheduler):
        """Tests that pending jobs are added (and if due, executed) when the scheduler starts."""

        freeze_time.set_increment(timedelta(seconds=0.2))
        scheduler.add_job(lambda x, y: x + y, 'date', args=[1, 2], run_date=freeze_time.next())
        start_scheduler()

        assert self.wait_event(eventqueue).code == EVENT_JOBSTORE_ADDED
        assert self.wait_event(eventqueue).code == EVENT_JOB_ADDED
        assert self.wait_event(eventqueue).code == EVENT_SCHEDULER_START
        event = self.wait_event(eventqueue)
        assert event.code == EVENT_JOB_EXECUTED
        assert event.retval == 3
        assert self.wait_event(eventqueue).code == EVENT_JOB_REMOVED

    def test_add_live_job(self, scheduler, freeze_time, eventqueue, start_scheduler):
        """Tests that adding a job causes it to be executed after the specified delay."""

        freeze_time.set_increment(timedelta(seconds=0.2))
        start_scheduler()
        assert self.wait_event(eventqueue).code == EVENT_JOBSTORE_ADDED
        assert self.wait_event(eventqueue).code == EVENT_SCHEDULER_START

        scheduler.add_job(lambda x, y: x + y, 'date', args=[1, 2],
                          run_date=freeze_time.next() + freeze_time.increment * 2)
        assert self.wait_event(eventqueue).code == EVENT_JOB_ADDED
        event = self.wait_event(eventqueue)
        assert event.code == EVENT_JOB_EXECUTED
        assert event.retval == 3
        assert self.wait_event(eventqueue).code == EVENT_JOB_REMOVED

    def test_shutdown(self, scheduler, eventqueue, start_scheduler):
        """Tests that shutting down the scheduler emits the proper event."""

        start_scheduler()
        assert self.wait_event(eventqueue).code == EVENT_JOBSTORE_ADDED
        assert self.wait_event(eventqueue).code == EVENT_SCHEDULER_START

        scheduler.shutdown()
        assert self.wait_event(eventqueue).code == EVENT_SCHEDULER_SHUTDOWN


class TestBlockingScheduler(SchedulerImplementationTestBase):
    @pytest.fixture
    def scheduler(self):
        from apscheduler.schedulers.blocking import BlockingScheduler
        return BlockingScheduler()

    @pytest.fixture
    def mock_event_scheduler(self, scheduler):
        scheduler._event = MagicMock()
        scheduler._event.clear.side_effect = StopIteration()
        return scheduler

    @pytest.fixture
    def start_scheduler(self, request, scheduler):
        def cleanup():
            if scheduler.running:
                scheduler.shutdown()
            thread.join()

        request.addfinalizer(cleanup)
        thread = Thread(target=scheduler.start)
        return thread.start

    @pytest.mark.parametrize('wait_seconds,expected_wait', [[0, 0], [None, 4294967], [728, 728]],
                             ids=['zero', 'none', 'positive'])
    def test_main_loop_wait_seconds_zero(self, mock_event_scheduler, wait_seconds, expected_wait):
        """Tests that _main_loop() correctly handles the condition where `wait_seconds` is 0."""

        mock_event_scheduler._process_jobs = MagicMock(return_value=wait_seconds)
        mock_event_scheduler._stopped = False
        try:  # We need this to break out of the infinite while loop
            mock_event_scheduler._main_loop()
        except StopIteration:
            pass

        mock_event_scheduler._event.wait.assert_called_with(expected_wait)


class TestBackgroundScheduler(SchedulerImplementationTestBase):
    @pytest.fixture
    def scheduler(self):
        from apscheduler.schedulers.background import BackgroundScheduler
        return BackgroundScheduler()


class TestAsyncIOScheduler(SchedulerImplementationTestBase):
    @pytest.fixture
    def event_loop(self):
        asyncio = pytest.importorskip('apscheduler.schedulers.asyncio')
        return asyncio.asyncio.new_event_loop()

    @pytest.fixture
    def scheduler(self, event_loop):
        asyncio = pytest.importorskip('apscheduler.schedulers.asyncio')
        return asyncio.AsyncIOScheduler(event_loop=event_loop)

    @pytest.fixture
    def start_scheduler(self, request, event_loop, scheduler):
        def cleanup():
            if scheduler.running:
                event_loop.call_soon_threadsafe(scheduler.shutdown)
            event_loop.call_soon_threadsafe(event_loop.stop)
            thread.join()

        event_loop.call_soon_threadsafe(scheduler.start)
        request.addfinalizer(cleanup)
        thread = Thread(target=event_loop.run_forever)
        return thread.start


class TestGeventScheduler(SchedulerImplementationTestBase):
    @pytest.fixture
    def scheduler(self):
        gevent = pytest.importorskip('apscheduler.schedulers.gevent')
        return gevent.GeventScheduler()

    @pytest.fixture
    def calc_event(self):
        from gevent.event import Event
        return Event()

    @pytest.fixture
    def eventqueue(self, scheduler):
        from gevent.queue import Queue
        events = Queue()
        scheduler.add_listener(events.put)
        return events


class TestTornadoScheduler(SchedulerImplementationTestBase):
    @pytest.fixture
    def io_loop(self):
        ioloop = pytest.importorskip('tornado.ioloop')
        return ioloop.IOLoop()

    @pytest.fixture
    def scheduler(self, io_loop):
        tornado = pytest.importorskip('apscheduler.schedulers.tornado')
        return tornado.TornadoScheduler(io_loop=io_loop)

    @pytest.fixture
    def start_scheduler(self, request, io_loop, scheduler):
        def cleanup():
            if scheduler.running:
                io_loop.add_callback(scheduler.shutdown)
            io_loop.add_callback(io_loop.stop)
            thread.join()

        io_loop.add_callback(scheduler.start)
        request.addfinalizer(cleanup)
        thread = Thread(target=io_loop.start)
        return thread.start


class TestTwistedScheduler(SchedulerImplementationTestBase):
    @pytest.fixture
    def reactor(self):
        selectreactor = pytest.importorskip('twisted.internet.selectreactor')
        return selectreactor.SelectReactor()

    @pytest.fixture
    def scheduler(self, reactor):
        twisted = pytest.importorskip('apscheduler.schedulers.twisted')
        return twisted.TwistedScheduler(reactor=reactor)

    @pytest.fixture
    def start_scheduler(self, request, reactor, scheduler):
        def cleanup():
            if scheduler.running:
                reactor.callFromThread(scheduler.shutdown)
            reactor.callFromThread(reactor.stop)
            thread.join()

        reactor.callFromThread(scheduler.start)
        request.addfinalizer(cleanup)
        thread = Thread(target=reactor.run, args=(False,))
        return thread.start


@pytest.mark.skip
class TestQtScheduler(SchedulerImplementationTestBase):
    @pytest.fixture(scope='class')
    def coreapp(self):
        QtCore = pytest.importorskip('PySide.QtCore')
        QtCore.QCoreApplication([])

    @pytest.fixture
    def scheduler(self, coreapp):
        qt = pytest.importorskip('apscheduler.schedulers.qt')
        return qt.QtScheduler()

    def wait_event(self, queue):
        from PySide.QtCore import QCoreApplication

        while queue.empty():
            QCoreApplication.processEvents()
        return queue.get_nowait()
