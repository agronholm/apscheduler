import logging
from datetime import datetime, timedelta
from threading import Thread

import pytest
import six
from pytz import utc

from apscheduler.events import (
    EVENT_SCHEDULER_STARTED, EVENT_SCHEDULER_SHUTDOWN, EVENT_JOBSTORE_ADDED,
    EVENT_JOBSTORE_REMOVED, EVENT_ALL, EVENT_ALL_JOBS_REMOVED, EVENT_EXECUTOR_ADDED,
    EVENT_EXECUTOR_REMOVED, EVENT_JOB_MODIFIED, EVENT_JOB_REMOVED, EVENT_JOB_ADDED,
    EVENT_JOB_EXECUTED, EVENT_JOB_SUBMITTED, EVENT_JOB_MAX_INSTANCES, EVENT_SCHEDULER_PAUSED,
    EVENT_SCHEDULER_RESUMED, SchedulerEvent)
from apscheduler.executors.base import BaseExecutor, MaxInstancesReachedError
from apscheduler.executors.debug import DebugExecutor
from apscheduler.job import Job
from apscheduler.jobstores.base import BaseJobStore, JobLookupError, ConflictingIdError
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.schedulers import SchedulerAlreadyRunningError, SchedulerNotRunningError
from apscheduler.schedulers.base import BaseScheduler, STATE_RUNNING, STATE_STOPPED
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


class DummyScheduler(BaseScheduler):
    def __init__(self, *args, **kwargs):
        super(DummyScheduler, self).__init__(*args, **kwargs)
        self.wakeup = MagicMock()

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
        self.start = MagicMock()
        self.shutdown = MagicMock()
        self.submit_job = MagicMock()

    def _do_submit_job(self, job, run_times):
        pass


class DummyJobStore(BaseJobStore):
    def __init__(self, **args):
        super(DummyJobStore, self).__init__()
        self.args = args
        self.start = MagicMock()
        self.shutdown = MagicMock()

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


class TestBaseScheduler(object):
    @pytest.fixture
    def scheduler(self, timezone):
        return DummyScheduler()

    @pytest.fixture
    def scheduler_events(self, request, scheduler):
        events = []
        mask = getattr(request, 'param', EVENT_ALL ^ EVENT_SCHEDULER_STARTED)
        scheduler.add_listener(events.append, mask)
        return events

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

    @pytest.mark.parametrize('method', [
        BaseScheduler.configure,
        BaseScheduler.start
    ])
    def test_scheduler_already_running(self, method, scheduler):
        """
        Test that SchedulerAlreadyRunningError is raised when certain methods are called before
        the scheduler has been started.

        """
        scheduler.start(paused=True)
        pytest.raises(SchedulerAlreadyRunningError, method, scheduler)

    @pytest.mark.parametrize('method', [
        BaseScheduler.pause,
        BaseScheduler.resume,
        BaseScheduler.shutdown
    ], ids=['pause', 'resume', 'shutdown'])
    def test_scheduler_not_running(self, scheduler, method):
        """
        Test that the SchedulerNotRunningError is raised when certain methods are called before
        the scheduler has been started.

        """
        pytest.raises(SchedulerNotRunningError, method, scheduler)

    def test_start(self, scheduler, create_job):
        scheduler._executors = {'exec1': MagicMock(BaseExecutor), 'exec2': MagicMock(BaseExecutor)}
        scheduler._jobstores = {'store1': MagicMock(BaseJobStore),
                                'store2': MagicMock(BaseJobStore)}
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

        scheduler._real_add_job.assert_called_once_with(job, 'store1', False)
        assert scheduler._pending_jobs == []

        assert scheduler._dispatch_event.call_count == 3
        event = scheduler._dispatch_event.call_args_list[0][0][0]
        assert event.code == EVENT_EXECUTOR_ADDED
        assert event.alias == 'default'
        event = scheduler._dispatch_event.call_args_list[1][0][0]
        assert event.code == EVENT_JOBSTORE_ADDED
        assert event.alias == 'default'
        event = scheduler._dispatch_event.call_args_list[2][0][0]
        assert event.code == EVENT_SCHEDULER_STARTED

        assert scheduler.state == STATE_RUNNING

    @pytest.mark.parametrize('wait', [True, False], ids=['wait', 'nowait'])
    def test_shutdown(self, scheduler, scheduler_events, wait):
        executor = DummyExecutor()
        jobstore = DummyJobStore()
        scheduler.add_executor(executor)
        scheduler.add_jobstore(jobstore)
        scheduler.start(paused=True)
        del scheduler_events[:]
        scheduler.shutdown(wait)

        assert scheduler.state == STATE_STOPPED
        assert len(scheduler_events) == 1
        assert scheduler_events[0].code == EVENT_SCHEDULER_SHUTDOWN

        executor.shutdown.assert_called_once_with(wait)
        jobstore.shutdown.assert_called_once_with()

    def test_pause_resume(self, scheduler, scheduler_events):
        scheduler.start()
        del scheduler_events[:]
        scheduler.wakeup.reset_mock()

        scheduler.pause()

        assert len(scheduler_events) == 1
        assert scheduler_events[0].code == EVENT_SCHEDULER_PAUSED
        assert not scheduler.wakeup.called

        scheduler.resume()
        assert len(scheduler_events) == 2
        assert scheduler_events[1].code == EVENT_SCHEDULER_RESUMED
        assert scheduler.wakeup.called

    @pytest.mark.parametrize('start_scheduler', [True, False])
    def test_running(self, scheduler, start_scheduler):
        if start_scheduler:
            scheduler.start()

        assert scheduler.running is start_scheduler

    @pytest.mark.parametrize('start_scheduler', [True, False])
    def test_add_remove_executor(self, scheduler, scheduler_events, start_scheduler):
        if start_scheduler:
            scheduler.start(paused=True)

        del scheduler_events[:]
        executor = DummyExecutor()
        scheduler.add_executor(executor, 'exec1')

        assert len(scheduler_events) == 1
        assert scheduler_events[0].code == EVENT_EXECUTOR_ADDED
        assert scheduler_events[0].alias == 'exec1'
        if start_scheduler:
            executor.start.assert_called_once_with(scheduler, 'exec1')
        else:
            assert not executor.start.called

        scheduler.remove_executor('exec1')
        assert len(scheduler_events) == 2
        assert scheduler_events[1].code == EVENT_EXECUTOR_REMOVED
        assert scheduler_events[1].alias == 'exec1'
        assert executor.shutdown.called

    def test_add_executor_already_exists(self, scheduler):
        executor = DummyExecutor()
        scheduler.add_executor(executor)
        exc = pytest.raises(ValueError, scheduler.add_executor, executor)
        assert str(exc.value) == 'This scheduler already has an executor by the alias of "default"'

    def test_remove_executor_nonexistent(self, scheduler):
        pytest.raises(KeyError, scheduler.remove_executor, 'foo')

    @pytest.mark.parametrize('start_scheduler', [True, False])
    def test_add_jobstore(self, scheduler, scheduler_events, start_scheduler):
        """
        Test that the proper event is dispatched when a job store is added and the scheduler's
        wake() method is called if the scheduler is running.

        """
        if start_scheduler:
            scheduler.start()

        del scheduler_events[:]
        jobstore = DummyJobStore()
        scheduler.add_jobstore(jobstore, 'store1')

        assert len(scheduler_events) == 1
        assert scheduler_events[0].code == EVENT_JOBSTORE_ADDED
        assert scheduler_events[0].alias == 'store1'

        if start_scheduler:
            assert scheduler.wakeup.called
            jobstore.start.assert_called_once_with(scheduler, 'store1')
        else:
            assert not jobstore.start.called

    def test_add_jobstore_already_exists(self, scheduler):
        """
        Test that ValueError is raised when a job store is added with an alias that already exists.

        """
        jobstore = MemoryJobStore()
        scheduler.add_jobstore(jobstore)
        exc = pytest.raises(ValueError, scheduler.add_jobstore, jobstore)
        assert str(exc.value) == 'This scheduler already has a job store by the alias of "default"'

    def test_remove_jobstore(self, scheduler, scheduler_events):
        scheduler.add_jobstore(MemoryJobStore(), 'foo')
        scheduler.remove_jobstore('foo')

        assert len(scheduler_events) == 2
        assert scheduler_events[1].code == EVENT_JOBSTORE_REMOVED
        assert scheduler_events[1].alias == 'foo'

    def test_remove_jobstore_nonexistent(self, scheduler):
        pytest.raises(KeyError, scheduler.remove_jobstore, 'foo')

    def test_add_remove_listener(self, scheduler):
        """Test that event dispatch works but removed listeners aren't called."""
        events = []
        scheduler.add_listener(events.append, EVENT_EXECUTOR_ADDED)
        scheduler.add_executor(DummyExecutor(), 'exec1')
        scheduler.remove_listener(events.append)
        scheduler.add_executor(DummyExecutor(), 'exec2')
        assert len(events) == 1

    def test_add_job_return_value(self, scheduler, timezone):
        """Test that when a job is added to a stopped scheduler, a Job instance is returned."""
        job = scheduler.add_job(lambda x, y: None, 'date', [1], {'y': 2}, 'my-id', 'dummy',
                                next_run_time=datetime(2014, 5, 23, 10),
                                run_date='2014-06-01 08:41:00')

        assert isinstance(job, Job)
        assert job.id == 'my-id'
        assert not hasattr(job, 'misfire_grace_time')
        assert not hasattr(job, 'coalesce')
        assert not hasattr(job, 'max_instances')
        assert job.next_run_time.tzinfo.zone == timezone.zone

    def test_add_job_pending(self, scheduler, scheduler_events):
        """
        Test that when a job is added to a stopped scheduler, it is not added to a job store until
        the scheduler is started and that the event is dispatched when that happens.

        """
        scheduler.configure(job_defaults={
            'misfire_grace_time': 3, 'coalesce': False, 'max_instances': 6
        })
        job = scheduler.add_job(lambda: None, 'interval', hours=1)
        assert not scheduler_events

        scheduler.start(paused=True)

        assert len(scheduler_events) == 3
        assert scheduler_events[2].code == EVENT_JOB_ADDED
        assert scheduler_events[2].job_id is job.id

        # Check that the undefined values were replaced with scheduler's job defaults
        assert job.misfire_grace_time == 3
        assert not job.coalesce
        assert job.max_instances == 6

    def test_add_job_id_conflict(self, scheduler):
        """
        Test that if a job is added with an already existing id, ConflictingIdError is raised.

        """
        scheduler.start(paused=True)
        scheduler.add_job(lambda: None, 'interval', id='testjob', seconds=1)
        pytest.raises(ConflictingIdError, scheduler.add_job, lambda: None, 'interval',
                      id='testjob', seconds=1)

    def test_add_job_replace(self, scheduler):
        """Test that with replace_existing=True, a new job replaces another with the same id."""
        scheduler.start(paused=True)
        scheduler.add_job(lambda: None, 'interval', id='testjob', seconds=1)
        scheduler.add_job(lambda: None, 'cron', id='testjob', name='replacement',
                          replace_existing=True)
        jobs = scheduler.get_jobs()
        assert len(jobs) == 1
        assert jobs[0].name == 'replacement'

    def test_scheduled_job(self, scheduler):
        def func(x, y):
            pass

        scheduler.add_job = MagicMock()
        decorator = scheduler.scheduled_job('date', [1], {'y': 2}, 'my-id',
                                            'dummy', run_date='2014-06-01 08:41:00')
        decorator(func)

        scheduler.add_job.assert_called_once_with(
            func, 'date', [1], {'y': 2}, 'my-id', 'dummy', undefined, undefined, undefined,
            undefined, 'default', 'default', True, run_date='2014-06-01 08:41:00')

    @pytest.mark.parametrize('pending', [True, False], ids=['pending job', 'scheduled job'])
    def test_modify_job(self, scheduler, pending, timezone):
        job = MagicMock()
        scheduler._dispatch_event = MagicMock()
        scheduler._lookup_job = MagicMock(return_value=(job, None if pending else 'default'))
        if not pending:
            jobstore = MagicMock()
            scheduler._lookup_jobstore = lambda alias: jobstore if alias == 'default' else None
        scheduler.modify_job('blah', misfire_grace_time=5, max_instances=2,
                             next_run_time=datetime(2014, 10, 17))

        job._modify.assert_called_once_with(misfire_grace_time=5, max_instances=2,
                                            next_run_time=datetime(2014, 10, 17))
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
            scheduler.modify_job.assert_called_once_with('foo', 'bar',
                                                         next_run_time=next_fire_time)

    @pytest.mark.parametrize('scheduler_started', [True, False], ids=['running', 'stopped'])
    @pytest.mark.parametrize('jobstore', [None, 'other'],
                             ids=['all jobstores', 'specific jobstore'])
    def test_get_jobs(self, scheduler, scheduler_started, jobstore):
        scheduler.add_jobstore(MemoryJobStore(), 'other')
        scheduler.add_job(lambda: None, 'interval', seconds=1, id='job1')
        scheduler.add_job(lambda: None, 'interval', seconds=1, id='job2', jobstore='other')
        if scheduler_started:
            scheduler.start(paused=True)

        expected_job_ids = {'job2'}
        if jobstore is None:
            expected_job_ids.add('job1')

        job_ids = {job.id for job in scheduler.get_jobs(jobstore)}
        assert job_ids == expected_job_ids

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

    @pytest.mark.parametrize('start_scheduler', [True, False])
    @pytest.mark.parametrize('jobstore', [None, 'other'],
                             ids=['any jobstore', 'specific jobstore'])
    def test_remove_job(self, scheduler, scheduler_events, start_scheduler, jobstore):
        scheduler.add_jobstore(MemoryJobStore(), 'other')
        scheduler.add_job(lambda: None, id='job1')
        if start_scheduler:
            scheduler.start(paused=True)

        del scheduler_events[:]
        if jobstore:
            pytest.raises(JobLookupError, scheduler.remove_job, 'job1', jobstore)
            assert len(scheduler.get_jobs()) == 1
            assert len(scheduler_events) == 0
        else:
            scheduler.remove_job('job1', jobstore)
            assert len(scheduler.get_jobs()) == 0
            assert len(scheduler_events) == 1
            assert scheduler_events[0].code == EVENT_JOB_REMOVED

    def test_remove_nonexistent_job(self, scheduler):
        pytest.raises(JobLookupError, scheduler.remove_job, 'foo')

    @pytest.mark.parametrize('start_scheduler', [True, False])
    @pytest.mark.parametrize('jobstore', [None, 'other'], ids=['all', 'single jobstore'])
    def test_remove_all_jobs(self, scheduler, start_scheduler, scheduler_events, jobstore):
        """
        Test that remove_all_jobs() removes all jobs from all attached job stores, plus any
        pending jobs.

        """
        scheduler.add_jobstore(MemoryJobStore(), 'other')
        scheduler.add_job(lambda: None, id='job1')
        scheduler.add_job(lambda: None, id='job2')
        scheduler.add_job(lambda: None, id='job3', jobstore='other')
        if start_scheduler:
            scheduler.start(paused=True)

        del scheduler_events[:]
        scheduler.remove_all_jobs(jobstore)
        jobs = scheduler.get_jobs()

        assert len(jobs) == (2 if jobstore else 0)
        assert len(scheduler_events) == 1
        assert scheduler_events[0].code == EVENT_ALL_JOBS_REMOVED
        assert scheduler_events[0].alias == jobstore

    @pytest.mark.parametrize('start_scheduler', [True, False])
    @pytest.mark.parametrize('jobstore', [None, 'other'],
                             ids=['all jobstores', 'specific jobstore'])
    def test_print_jobs(self, scheduler, start_scheduler, jobstore):
        scheduler.add_jobstore(MemoryJobStore(), 'other')
        if start_scheduler:
            scheduler.start(paused=True)

        scheduler.add_job(lambda: None, 'date', run_date='2099-09-09', id='job1',
                          name='test job 1')
        scheduler.add_job(lambda: None, 'date', run_date='2099-08-08', id='job2',
                          name='test job 2', jobstore='other')

        outfile = StringIO()
        scheduler.print_jobs(jobstore, outfile)

        if jobstore and not start_scheduler:
            assert outfile.getvalue() == """\
Pending jobs:
    test job 2 (trigger: date[2099-08-08 00:00:00 CET], pending)
"""
        elif jobstore and start_scheduler:
            assert outfile.getvalue() == """\
Jobstore other:
    test job 2 (trigger: date[2099-08-08 00:00:00 CET], next run at: 2099-08-08 00:00:00 CET)
"""
        elif not jobstore and not start_scheduler:
            assert outfile.getvalue() == """\
Pending jobs:
    test job 1 (trigger: date[2099-09-09 00:00:00 CET], pending)
    test job 2 (trigger: date[2099-08-08 00:00:00 CET], pending)
"""
        else:
            assert outfile.getvalue() == """\
Jobstore default:
    test job 1 (trigger: date[2099-09-09 00:00:00 CET], next run at: 2099-09-09 00:00:00 CET)
Jobstore other:
    test job 2 (trigger: date[2099-08-08 00:00:00 CET], next run at: 2099-08-08 00:00:00 CET)
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
        assert str(exc.value) == ("Expected executor instance or dict for executors['default'], "
                                  "got int instead")

    def test_configure_private_invalid_jobstore(self, scheduler):
        exc = pytest.raises(TypeError, scheduler._configure, {'jobstores': {'default': 6}})
        assert str(exc.value) == ("Expected job store instance or dict for jobstores['default'], "
                                  "got int instead")

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

    def test_dispatch_event(self, scheduler):
        event = SchedulerEvent(1)
        scheduler._listeners = [(MagicMock(), 2), (MagicMock(side_effect=Exception), 1),
                                (MagicMock(), 1)]
        scheduler._dispatch_event(event)

        assert not scheduler._listeners[0][0].called
        scheduler._listeners[1][0].assert_called_once_with(event)

    @pytest.mark.parametrize('load_plugin', [True, False], ids=['load plugin', 'plugin loaded'])
    def test_create_trigger(self, scheduler, load_plugin):
        """Tests that creating a trigger with an already loaded plugin works."""

        scheduler._trigger_plugins = {}
        scheduler._trigger_classes = {}
        if load_plugin:
            scheduler._trigger_plugins['dummy'] = MagicMock(
                load=MagicMock(return_value=DummyTrigger))
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

    def test_job_submitted_event(self, scheduler, freeze_time):
        events = []
        scheduler.add_job(lambda: None, run_date=freeze_time.get())
        scheduler.add_listener(events.append, EVENT_JOB_SUBMITTED)
        scheduler.start()
        scheduler._process_jobs()

        assert len(events) == 1
        assert events[0].scheduled_run_times == [freeze_time.get(scheduler.timezone)]

    @pytest.mark.parametrize('scheduler_events', [EVENT_JOB_MAX_INSTANCES],
                             indirect=['scheduler_events'])
    def test_job_max_instances_event(self, scheduler, scheduler_events, freeze_time):
        class MaxedOutExecutor(DebugExecutor):
            def submit_job(self, job, run_times):
                raise MaxInstancesReachedError(job)

        executor = MaxedOutExecutor()
        scheduler.add_executor(executor, 'maxed')
        scheduler.add_job(lambda: None, run_date=freeze_time.get(), executor='maxed')
        scheduler.start()
        scheduler._process_jobs()

        assert len(scheduler_events) == 1
        assert scheduler_events[0].scheduled_run_times == [freeze_time.get(scheduler.timezone)]


class TestProcessJobs(object):
    @pytest.fixture
    def job(self):
        job = MagicMock(Job, id=999, executor='default')
        job.trigger = MagicMock(get_next_fire_time=MagicMock(return_value=None))
        job. __str__ = lambda x: 'job 999'
        return job

    @pytest.fixture
    def scheduler(self):
        scheduler = DummyScheduler()
        scheduler.start()
        return scheduler

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

    def test_nonexistent_executor(self, scheduler, jobstore, caplog):
        """
        Test that an error is logged and the job is removed from its job store if its executor is
        not found.

        """
        caplog.set_level(logging.ERROR)
        scheduler.remove_executor('default')
        assert scheduler._process_jobs() is None
        jobstore.remove_job.assert_called_once_with(999)
        assert len(caplog.records) == 1
        assert caplog.records[0].message == \
            'Executor lookup ("default") failed for job "job 999" -- removing it from the job ' \
            'store'

    def test_max_instances_reached(self, scheduler, job, jobstore, executor, caplog):
        """Tests that a warning is logged when the maximum instances of a job is reached."""
        caplog.set_level(logging.WARNING)
        executor.submit_job = MagicMock(side_effect=MaxInstancesReachedError(job))

        assert scheduler._process_jobs() is None
        assert len(caplog.records) == 1
        assert caplog.records[0].message == \
            'Execution of job "job 999" skipped: maximum number of running instances reached (1)'

    def test_executor_error(self, scheduler, jobstore, executor, caplog):
        """Tests that if any exception is raised in executor.submit(), it is logged."""
        caplog.set_level(logging.ERROR)
        executor.submit_job = MagicMock(side_effect=Exception('test message'))

        assert scheduler._process_jobs() is None
        assert len(caplog.records) == 1
        assert 'test message' in caplog.records[0].exc_text
        assert 'Error submitting job "job 999" to executor "default"' in caplog.records[0].message

    def test_job_update(self, scheduler, job, jobstore, freeze_time):
        """
        Tests that the job is updated in its job store with the next run time from the trigger.

        """
        next_run_time = freeze_time.current + timedelta(seconds=6)
        job.trigger.get_next_fire_time = MagicMock(return_value=next_run_time)
        assert scheduler._process_jobs() is None
        job._modify.assert_called_once_with(next_run_time=next_run_time)
        jobstore.update_job.assert_called_once_with(job)

    def test_wait_time(self, scheduler, freeze_time):
        """
        Tests that the earliest next run time from all job stores is returned (ignoring Nones).

        """
        scheduler._jobstores = {
            'default': MagicMock(get_next_run_time=MagicMock(
                return_value=freeze_time.current + timedelta(seconds=8))),
            'alter': MagicMock(get_next_run_time=MagicMock(return_value=None)),
            'another': MagicMock(get_next_run_time=MagicMock(
                return_value=freeze_time.current + timedelta(seconds=5))),
            'more': MagicMock(get_next_run_time=MagicMock(
                return_value=freeze_time.current + timedelta(seconds=6))),
        }

        assert scheduler._process_jobs() == 5


class SchedulerImplementationTestBase(object):
    @pytest.fixture(autouse=True)
    def executor(self, scheduler):
        scheduler.add_executor(DebugExecutor())

    @pytest.yield_fixture
    def start_scheduler(self, request, scheduler):
        yield scheduler.start
        if scheduler.running:
            scheduler.shutdown()

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
        assert self.wait_event(eventqueue).code == EVENT_SCHEDULER_STARTED
        event = self.wait_event(eventqueue)
        assert event.code == EVENT_JOB_EXECUTED
        assert event.retval == 3
        assert self.wait_event(eventqueue).code == EVENT_JOB_REMOVED

    def test_add_live_job(self, scheduler, freeze_time, eventqueue, start_scheduler):
        """Tests that adding a job causes it to be executed after the specified delay."""
        freeze_time.set_increment(timedelta(seconds=0.2))
        start_scheduler()
        assert self.wait_event(eventqueue).code == EVENT_JOBSTORE_ADDED
        assert self.wait_event(eventqueue).code == EVENT_SCHEDULER_STARTED

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
        assert self.wait_event(eventqueue).code == EVENT_SCHEDULER_STARTED

        scheduler.shutdown()
        assert self.wait_event(eventqueue).code == EVENT_SCHEDULER_SHUTDOWN


class TestBlockingScheduler(SchedulerImplementationTestBase):
    @pytest.fixture
    def scheduler(self):
        from apscheduler.schedulers.blocking import BlockingScheduler
        return BlockingScheduler()

    @pytest.yield_fixture
    def start_scheduler(self, request, scheduler):
        thread = Thread(target=scheduler.start)
        yield thread.start

        if scheduler.running:
            scheduler.shutdown()
        thread.join()


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

    @pytest.yield_fixture
    def start_scheduler(self, request, event_loop, scheduler):
        event_loop.call_soon_threadsafe(scheduler.start)
        thread = Thread(target=event_loop.run_forever)
        yield thread.start

        if scheduler.running:
            event_loop.call_soon_threadsafe(scheduler.shutdown)
        event_loop.call_soon_threadsafe(event_loop.stop)
        thread.join()


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

    @pytest.yield_fixture
    def start_scheduler(self, request, io_loop, scheduler):
        io_loop.add_callback(scheduler.start)
        thread = Thread(target=io_loop.start)
        yield thread.start

        if scheduler.running:
            io_loop.add_callback(scheduler.shutdown)
        io_loop.add_callback(io_loop.stop)
        thread.join()


class TestTwistedScheduler(SchedulerImplementationTestBase):
    @pytest.fixture
    def reactor(self):
        selectreactor = pytest.importorskip('twisted.internet.selectreactor')
        return selectreactor.SelectReactor()

    @pytest.fixture
    def scheduler(self, reactor):
        twisted = pytest.importorskip('apscheduler.schedulers.twisted')
        return twisted.TwistedScheduler(reactor=reactor)

    @pytest.yield_fixture
    def start_scheduler(self, request, reactor, scheduler):
        reactor.callFromThread(scheduler.start)
        thread = Thread(target=reactor.run, args=(False,))
        yield thread.start

        if scheduler.running:
            reactor.callFromThread(scheduler.shutdown)
        reactor.callFromThread(reactor.stop)
        thread.join()


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
