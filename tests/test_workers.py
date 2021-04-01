import threading
from datetime import datetime

import pytest
from anyio import Event, fail_after
from apscheduler.abc import Job
from apscheduler.events import JobDeadlineMissed, JobFailed, JobSuccessful, JobUpdated
from apscheduler.workers.async_ import AsyncWorker
from apscheduler.workers.sync import SyncWorker

pytestmark = pytest.mark.anyio


def sync_func(*args, fail: bool, **kwargs):
    if fail:
        raise Exception('failing as requested')
    else:
        return args, kwargs


async def async_func(*args, fail: bool, **kwargs):
    if fail:
        raise Exception('failing as requested')
    else:
        return args, kwargs


def fail_func():
    pytest.fail('This function should never be run')


class TestAsyncWorker:
    @pytest.mark.parametrize('target_func', [sync_func, async_func], ids=['sync', 'async'])
    @pytest.mark.parametrize('fail', [False, True], ids=['success', 'fail'])
    @pytest.mark.parametrize('anyio_backend', ['asyncio'])
    async def test_run_job_nonscheduled_success(self, target_func, fail, store):
        async def listener(worker_event):
            worker_events.append(worker_event)
            if len(worker_events) == 2:
                event.set()

        worker_events = []
        event = Event()
        job = Job('task_id', func=target_func, args=(1, 2), kwargs={'x': 'foo', 'fail': fail})
        async with AsyncWorker(store) as worker:
            worker.subscribe(listener)
            await store.add_job(job)
            with fail_after(2):
                await event.wait()

        assert len(worker_events) == 2

        assert isinstance(worker_events[0], JobUpdated)
        assert worker_events[0].job_id == job.id
        assert worker_events[0].task_id == 'task_id'
        assert worker_events[0].schedule_id is None

        assert worker_events[1].job_id == job.id
        assert worker_events[1].task_id == 'task_id'
        assert worker_events[1].schedule_id is None
        if fail:
            assert isinstance(worker_events[1], JobFailed)
            assert type(worker_events[1].exception) is Exception
            assert isinstance(worker_events[1].traceback, str)
        else:
            assert isinstance(worker_events[1], JobSuccessful)
            assert worker_events[1].return_value == ((1, 2), {'x': 'foo'})

    @pytest.mark.parametrize('anyio_backend', ['asyncio'])
    async def test_run_deadline_missed(self, store):
        async def listener(worker_event):
            worker_events.append(worker_event)
            event.set()

        scheduled_start_time = datetime(2020, 9, 14)
        worker_events = []
        event = Event()
        job = Job('task_id', fail_func, args=(), kwargs={}, schedule_id='foo',
                  scheduled_fire_time=scheduled_start_time,
                  start_deadline=datetime(2020, 9, 14, 1))
        async with AsyncWorker(store) as worker:
            worker.subscribe(listener)
            await store.add_job(job)
            with fail_after(5):
                await event.wait()

        assert len(worker_events) == 1
        assert isinstance(worker_events[0], JobDeadlineMissed)
        assert worker_events[0].job_id == job.id
        assert worker_events[0].task_id == 'task_id'
        assert worker_events[0].schedule_id == 'foo'
        assert worker_events[0].scheduled_fire_time == scheduled_start_time


class TestSyncWorker:
    @pytest.mark.parametrize('target_func', [sync_func, async_func], ids=['sync', 'async'])
    @pytest.mark.parametrize('fail', [False, True], ids=['success', 'fail'])
    def test_run_job_nonscheduled(self, anyio_backend, target_func, fail, sync_store, portal):
        def listener(worker_event):
            print('received event:', worker_event)
            worker_events.append(worker_event)
            if len(worker_events) == 2:
                event.set()

        worker_events = []
        event = threading.Event()
        job = Job('task_id', func=target_func, args=(1, 2), kwargs={'x': 'foo', 'fail': fail})
        with SyncWorker(sync_store, portal=portal) as worker:
            worker.subscribe(listener)
            portal.call(sync_store.add_job, job)
            event.wait(2)

        assert len(worker_events) == 2

        assert isinstance(worker_events[0], JobUpdated)
        assert worker_events[0].job_id == job.id
        assert worker_events[0].task_id == 'task_id'
        assert worker_events[0].schedule_id is None

        assert worker_events[1].job_id == job.id
        assert worker_events[1].task_id == 'task_id'
        assert worker_events[1].schedule_id is None
        if fail:
            assert isinstance(worker_events[1], JobFailed)
            assert type(worker_events[1].exception) is Exception
            assert isinstance(worker_events[1].traceback, str)
        else:
            assert isinstance(worker_events[1], JobSuccessful)
            assert worker_events[1].return_value == ((1, 2), {'x': 'foo'})

    def test_run_deadline_missed(self, anyio_backend, sync_store, portal):
        def listener(worker_event):
            worker_events.append(worker_event)
            event.set()

        scheduled_start_time = datetime(2020, 9, 14)
        worker_events = []
        event = threading.Event()
        job = Job('task_id', fail_func, args=(), kwargs={}, schedule_id='foo',
                  scheduled_fire_time=scheduled_start_time,
                  start_deadline=datetime(2020, 9, 14, 1))
        with SyncWorker(sync_store, portal=portal) as worker:
            worker.subscribe(listener)
            portal.call(sync_store.add_job, job)
            event.wait(5)

        assert len(worker_events) == 1
        assert isinstance(worker_events[0], JobDeadlineMissed)
        assert worker_events[0].job_id == job.id
        assert worker_events[0].task_id == 'task_id'
        assert worker_events[0].schedule_id == 'foo'
