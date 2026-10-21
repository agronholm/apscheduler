from datetime import datetime, timedelta, timezone

import pytest

from apscheduler.events import (
    EVENT_JOB_ERROR,
    EVENT_JOB_EXECUTED,
    EVENT_JOB_MISSED,
    EVENT_JOB_SUBMITTED,
)
from apscheduler.schedulers.base import BaseScheduler


class ManualScheduler(BaseScheduler):
    def wakeup(self):
        pass

    def shutdown(self, wait=True):
        super().shutdown(wait)


def succeed():
    return "done"


def fail():
    raise ValueError("job failed")


def complete_pool_jobs_inline(monkeypatch, scheduler):
    pool = scheduler._executors["default"]._pool
    submit = pool.submit

    def complete_before_callback(*args, **kwargs):
        future = submit(*args, **kwargs)
        future.result(timeout=3)
        return future

    # Use a real worker and completed Future, forcing add_done_callback() inline.
    monkeypatch.setattr(pool, "submit", complete_before_callback)


@pytest.mark.parametrize(
    "func,lateness,event_code",
    [
        (succeed, timedelta(), EVENT_JOB_EXECUTED),
        (fail, timedelta(), EVENT_JOB_ERROR),
        (
            succeed,
            timedelta(days=1),
            EVENT_JOB_MISSED,
        ),
    ],
    ids=["executed", "error", "missed"],
)
def test_inline_pool_completion_follows_submission(
    monkeypatch, func, lateness, event_code
):
    scheduler = ManualScheduler(timezone=timezone.utc)
    events = []
    scheduler.add_listener(
        lambda event: events.append(event), EVENT_JOB_SUBMITTED | event_code
    )
    scheduler.add_job(
        func,
        "date",
        run_date=datetime.now(timezone.utc) - lateness,
        id="fast",
        misfire_grace_time=1 if event_code == EVENT_JOB_MISSED else None,
    )
    scheduler.start()
    complete_pool_jobs_inline(monkeypatch, scheduler)
    try:
        scheduler._process_jobs()
    finally:
        scheduler.shutdown()

    assert [event.code for event in events] == [EVENT_JOB_SUBMITTED, event_code]
    assert events[0].job_id == events[1].job_id == "fast"
    assert events[0].scheduled_run_times == [events[1].scheduled_run_time]


@pytest.mark.parametrize("coalesce,occurrences", [(False, 3), (True, 1)])
def test_each_interval_occurrence_follows_submission(
    monkeypatch, coalesce, occurrences
):
    now = datetime(2020, 1, 1, tzinfo=timezone.utc)

    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return now.astimezone(tz) if tz else now.replace(tzinfo=None)

    monkeypatch.setattr("apscheduler.schedulers.base.datetime", FrozenDateTime)
    scheduler = ManualScheduler(timezone=timezone.utc)
    events = []
    scheduler.add_listener(
        lambda event: events.append(event), EVENT_JOB_SUBMITTED | EVENT_JOB_EXECUTED
    )
    scheduler.add_job(
        succeed,
        "interval",
        seconds=1,
        id="repeated",
        next_run_time=now - timedelta(seconds=2),
        coalesce=coalesce,
        misfire_grace_time=None,
    )
    scheduler.start()
    complete_pool_jobs_inline(monkeypatch, scheduler)
    try:
        scheduler._process_jobs()
        assert [event.code for event in events] == [EVENT_JOB_SUBMITTED] + [
            EVENT_JOB_EXECUTED
        ] * occurrences
        assert events[0].scheduled_run_times == [
            event.scheduled_run_time for event in events[1:]
        ]
        assert scheduler._pending_job_events == {}

        events.clear()
        scheduler.modify_job("repeated", next_run_time=now)
        scheduler._process_jobs()
        assert [event.code for event in events] == [
            EVENT_JOB_SUBMITTED,
            EVENT_JOB_EXECUTED,
        ]
        assert scheduler._pending_job_events == {}
    finally:
        scheduler.shutdown()


def test_same_job_id_in_different_stores_keeps_both_completions(monkeypatch):
    scheduler = ManualScheduler(timezone=timezone.utc)
    scheduler.add_jobstore("memory", "other")
    events = []
    scheduler.add_listener(
        lambda event: events.append(event), EVENT_JOB_SUBMITTED | EVENT_JOB_EXECUTED
    )
    run_date = datetime.now(timezone.utc)
    for store in ("default", "other"):
        scheduler.add_job(
            succeed,
            "date",
            run_date=run_date,
            id="same",
            jobstore=store,
            misfire_grace_time=None,
        )

    scheduler.start()
    complete_pool_jobs_inline(monkeypatch, scheduler)
    try:
        scheduler._process_jobs()
    finally:
        scheduler.shutdown()

    for store in ("default", "other"):
        assert [event.code for event in events if event.jobstore == store] == [
            EVENT_JOB_SUBMITTED,
            EVENT_JOB_EXECUTED,
        ]
    assert scheduler._pending_job_events == {}
