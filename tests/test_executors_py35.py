"""Contains test functions using Python 3.3+ syntax."""
from datetime import datetime

import pytest
from apscheduler.executors.asyncio import AsyncIOExecutor
from apscheduler.executors.tornado import TornadoExecutor
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.schedulers.tornado import TornadoScheduler
from pytz import utc


@pytest.fixture
def asyncio_scheduler(event_loop):
    scheduler = AsyncIOScheduler(event_loop=event_loop)
    scheduler.start(paused=True)
    yield scheduler
    scheduler.shutdown(False)


@pytest.fixture
def asyncio_executor(asyncio_scheduler):
    executor = AsyncIOExecutor()
    executor.start(asyncio_scheduler, 'default')
    yield executor
    executor.shutdown()


@pytest.fixture
def tornado_scheduler(io_loop):
    scheduler = TornadoScheduler(io_loop=io_loop)
    scheduler.start(paused=True)
    yield scheduler
    scheduler.shutdown(False)


@pytest.fixture
def tornado_executor(tornado_scheduler):
    executor = TornadoExecutor()
    executor.start(tornado_scheduler, 'default')
    yield executor
    executor.shutdown()


async def waiter(sleep, exception):
    await sleep(0.1)
    if exception:
        raise Exception('dummy error')
    else:
        return True


@pytest.mark.parametrize('exception', [False, True])
@pytest.mark.asyncio
async def test_run_coroutine_job(asyncio_scheduler, asyncio_executor, exception):
    from asyncio import Future, sleep

    future = Future()
    job = asyncio_scheduler.add_job(waiter, 'interval', seconds=1, args=[sleep, exception])
    asyncio_executor._run_job_success = lambda job_id, events: future.set_result(events)
    asyncio_executor._run_job_error = lambda job_id, exc, tb: future.set_exception(exc)
    asyncio_executor.submit_job(job, [datetime.now(utc)])
    events = await future
    assert len(events) == 1
    if exception:
        assert str(events[0].exception) == 'dummy error'
    else:
        assert events[0].retval is True


@pytest.mark.parametrize('exception', [False, True])
@pytest.mark.gen_test
async def test_run_coroutine_job_tornado(tornado_scheduler, tornado_executor, exception):
    from tornado.concurrent import Future
    from tornado.gen import sleep

    future = Future()
    job = tornado_scheduler.add_job(waiter, 'interval', seconds=1, args=[sleep, exception])
    tornado_executor._run_job_success = lambda job_id, events: future.set_result(events)
    tornado_executor._run_job_error = lambda job_id, exc, tb: future.set_exception(exc)
    tornado_executor.submit_job(job, [datetime.now(utc)])
    events = await future
    assert len(events) == 1
    if exception:
        assert str(events[0].exception) == 'dummy error'
    else:
        assert events[0].retval is True
