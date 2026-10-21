import multiprocessing
import traceback
from datetime import datetime, timedelta, timezone
from threading import Event

import pytest

from apscheduler.events import (
    EVENT_JOB_EXECUTED,
    EVENT_JOB_MAX_INSTANCES,
    EVENT_JOB_REMOVED,
    EVENT_JOB_SUBMITTED,
)
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.schedulers.base import BaseScheduler


class ManualScheduler(BaseScheduler):
    def wakeup(self):
        pass

    def shutdown(self, wait=True):
        super().shutdown(wait)


def _reentry_case(action, connection):
    """Keep a listener deadlock from leaving pool threads in the pytest process."""
    scheduler = ManualScheduler(timezone=timezone.utc)
    worker_started = Event()
    worker_release = Event()
    callback_finished = Event()
    listener_finished = Event()
    received = []

    def job():
        worker_started.set()
        assert worker_release.wait(3)
        return "finished"

    try:
        executor = ThreadPoolExecutor(max_workers=1)
        scheduler.add_executor(executor)
        scheduler.start(paused=True)
        original_success = executor._run_job_success

        def run_job_success(job_id, events):
            try:
                original_success(job_id, events)
            finally:
                callback_finished.set()

        executor._run_job_success = run_job_success
        scheduler.add_job(lambda: None, id="other", next_run_time=None)

        def first_listener(event):
            assert worker_started.wait(3)
            if action == "mutate":
                scheduler.remove_job("other")
                scheduler.add_job(lambda: None, id="added", next_run_time=None)
                assert scheduler.get_job("added") is not None
                scheduler.remove_job("added")

            worker_release.set()
            if action == "shutdown":
                scheduler.shutdown(wait=True)
            else:
                assert callback_finished.wait(3)

            listener_finished.set()
            if action == "raise":
                raise RuntimeError("listener failure must not stop delivery")

        scheduler.add_listener(first_listener, EVENT_JOB_SUBMITTED)
        scheduler.add_listener(
            received.append, EVENT_JOB_SUBMITTED | EVENT_JOB_EXECUTED
        )
        run_time = datetime.now(timezone.utc) - timedelta(milliseconds=1)
        scheduler.add_job(
            job, "date", id="due", run_date=run_time, misfire_grace_time=None
        )
        scheduler.resume()
        scheduler._process_jobs()

        assert callback_finished.wait(3)
        assert listener_finished.is_set()
        assert [event.code for event in received] == [
            EVENT_JOB_SUBMITTED,
            EVENT_JOB_EXECUTED,
        ]
        assert received[0].scheduled_run_times == [run_time]
        assert received[1].scheduled_run_time == run_time
        assert received[1].retval == "finished"
        if action == "mutate":
            assert scheduler.get_jobs() == []

        connection.send(None)
    except Exception:
        connection.send(traceback.format_exc())
    finally:
        worker_release.set()
        if scheduler.running:
            scheduler.shutdown(wait=True)
        connection.close()


def _run_in_child(target, *args):
    context = multiprocessing.get_context("spawn")
    receiving, sending = context.Pipe(duplex=False)
    process = context.Process(target=target, args=(*args, sending))
    process.start()
    sending.close()
    try:
        process.join(8)
        if process.is_alive():
            process.terminate()
            process.join(3)
            pytest.fail("The event listener deadlocked")

        assert process.exitcode == 0
        assert receiving.poll(), "The child process did not report its assertions"
        failure = receiving.recv()
        assert failure is None, failure
    finally:
        if process.is_alive():
            process.kill()
            process.join(3)
        receiving.close()
        process.close()


@pytest.mark.timeout(15)
@pytest.mark.parametrize("action", ["shutdown", "mutate", "raise"])
def test_submission_listener_reentry(action):
    _run_in_child(_reentry_case, action)


def _recursive_replacement_case(connection):
    scheduler = ManualScheduler(timezone=timezone.utc)
    received = []
    calls = []
    replacement_added = False
    run_time = datetime.now(timezone.utc) - timedelta(milliseconds=1)

    def job():
        calls.append(len(calls) + 1)
        return calls[-1]

    def removed_listener(event):
        nonlocal replacement_added
        if event.job_id == "due" and not replacement_added:
            replacement_added = True
            scheduler.add_job(
                job, "date", id="due", run_date=run_time, misfire_grace_time=None
            )
            scheduler._process_jobs()

    try:
        scheduler.add_job(
            job, "date", id="due", run_date=run_time, misfire_grace_time=None
        )
        scheduler.start()
        pool = scheduler._executors["default"]._pool
        original_submit = pool.submit

        def complete_before_callback(*args, **kwargs):
            future = original_submit(*args, **kwargs)
            future.result(timeout=3)
            return future

        pool.submit = complete_before_callback
        scheduler.add_listener(removed_listener, EVENT_JOB_REMOVED)
        scheduler.add_listener(
            received.append, EVENT_JOB_SUBMITTED | EVENT_JOB_EXECUTED
        )
        scheduler._process_jobs()

        assert calls == [1, 2]
        assert [event.code for event in received] == [
            EVENT_JOB_SUBMITTED,
            EVENT_JOB_SUBMITTED,
            EVENT_JOB_EXECUTED,
            EVENT_JOB_EXECUTED,
        ]
        assert sorted(
            event.retval for event in received if event.code == EVENT_JOB_EXECUTED
        ) == [1, 2]
        assert scheduler._pending_job_events == {}
        connection.send(None)
    except Exception:
        connection.send(traceback.format_exc())
    finally:
        if scheduler.running:
            scheduler.shutdown(wait=True)
        connection.close()


@pytest.mark.timeout(15)
def test_removed_listener_recursively_replaces_the_same_run():
    _run_in_child(_recursive_replacement_case)


@pytest.mark.timeout(8)
def test_dispatch_override_observes_each_event_once_after_submission(monkeypatch):
    class ObservedScheduler(ManualScheduler):
        def __init__(self):
            self.observed = []
            super().__init__(timezone=timezone.utc)

        def _dispatch_event(self, event):
            if event.code in (EVENT_JOB_SUBMITTED, EVENT_JOB_EXECUTED):
                self.observed.append(event.code)
            super()._dispatch_event(event)

    scheduler = ObservedScheduler()
    received = []
    scheduler.add_listener(
        lambda event: received.append(event.code),
        EVENT_JOB_SUBMITTED | EVENT_JOB_EXECUTED,
    )
    scheduler.add_job(lambda: None, "date", misfire_grace_time=None)
    scheduler.start()
    pool = scheduler._executors["default"]._pool
    original_submit = pool.submit

    def complete_before_callback(*args, **kwargs):
        future = original_submit(*args, **kwargs)
        future.result(timeout=3)
        return future

    monkeypatch.setattr(pool, "submit", complete_before_callback)
    try:
        scheduler._process_jobs()
    finally:
        scheduler.shutdown(wait=True)

    assert received == [EVENT_JOB_SUBMITTED, EVENT_JOB_EXECUTED]
    assert scheduler.observed == received


@pytest.fixture
def manual_pool_scheduler():
    scheduler = ManualScheduler(timezone=timezone.utc)
    executor = ThreadPoolExecutor(max_workers=1)
    scheduler.add_executor(executor)
    scheduler.start(paused=True)
    yield scheduler, executor
    if scheduler.running:
        scheduler.shutdown(wait=True)


@pytest.mark.timeout(8)
def test_rejected_submission_cleans_pending_events(manual_pool_scheduler, monkeypatch):
    scheduler, executor = manual_pool_scheduler

    def reject(*args, **kwargs):
        raise RuntimeError("submission rejected")

    monkeypatch.setattr(executor._pool, "submit", reject)
    received = []
    scheduler.add_listener(received.append, EVENT_JOB_SUBMITTED)
    scheduler.add_job(lambda: None, "date", misfire_grace_time=None)
    scheduler.resume()
    scheduler._process_jobs()

    assert received == []
    assert scheduler._pending_job_events == {}


@pytest.mark.timeout(8)
def test_max_instances_cleans_pending_events(manual_pool_scheduler):
    scheduler, _executor = manual_pool_scheduler
    worker_started = Event()
    worker_release = Event()
    executed = Event()
    received = []

    def job():
        worker_started.set()
        assert worker_release.wait(3)

    def listener(event):
        received.append(event.code)
        if event.code == EVENT_JOB_EXECUTED:
            executed.set()

    scheduler.add_listener(
        listener, EVENT_JOB_SUBMITTED | EVENT_JOB_EXECUTED | EVENT_JOB_MAX_INSTANCES
    )
    run_time = datetime.now(timezone.utc) - timedelta(milliseconds=1)
    scheduler.add_job(
        job,
        "interval",
        seconds=60,
        id="due",
        next_run_time=run_time,
        misfire_grace_time=None,
    )
    try:
        scheduler.resume()
        scheduler._process_jobs()
        assert worker_started.wait(3)
        scheduler.modify_job("due", next_run_time=run_time)
        scheduler._process_jobs()
        assert scheduler._pending_job_events == {}
        worker_release.set()
        assert executed.wait(3)
        assert received == [
            EVENT_JOB_SUBMITTED,
            EVENT_JOB_MAX_INSTANCES,
            EVENT_JOB_EXECUTED,
        ]
    finally:
        worker_release.set()


@pytest.mark.timeout(8)
@pytest.mark.parametrize("method", ["update_job", "get_next_run_time"])
def test_jobstore_failure_releases_completion(
    manual_pool_scheduler, monkeypatch, method
):
    scheduler, executor = manual_pool_scheduler
    worker_release = Event()
    received = []

    def job():
        assert worker_release.wait(3)

    def reject(*args):
        raise RuntimeError("jobstore failure")

    def listener(event):
        received.append(event.code)
        if event.code == EVENT_JOB_SUBMITTED:
            worker_release.set()

    jobstore = scheduler._jobstores["default"]
    assert isinstance(jobstore, MemoryJobStore)
    scheduler.add_listener(listener, EVENT_JOB_SUBMITTED | EVENT_JOB_EXECUTED)
    scheduler.add_job(
        job,
        "interval",
        seconds=60,
        next_run_time=datetime.now(timezone.utc) - timedelta(milliseconds=1),
        misfire_grace_time=None,
    )
    monkeypatch.setattr(jobstore, method, reject)
    scheduler.resume()
    try:
        with pytest.raises(RuntimeError, match="jobstore failure"):
            scheduler._process_jobs()
    finally:
        worker_release.set()
        executor.shutdown(wait=True)

    assert received == [EVENT_JOB_SUBMITTED, EVENT_JOB_EXECUTED]
    assert scheduler._pending_job_events == {}
