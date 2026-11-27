from __future__ import annotations

from apscheduler import AsyncScheduler, Scheduler
from apscheduler.executors.async_ import AsyncJobExecutor


def test_sync_scheduler_accepts_custom_executor_only_mapping() -> None:
    """Regression: the sync wrapper must not inject a non-existent executor name."""
    executor = AsyncJobExecutor()
    scheduler = Scheduler(job_executors={"custom": executor})

    assert scheduler.job_executors == {"custom": executor}
    assert scheduler.task_defaults.job_executor == "custom"


def test_async_scheduler_still_accepts_custom_executor_only_mapping() -> None:
    """Parity check: the async wrapper already handled this mapping."""
    executor = AsyncJobExecutor()
    scheduler = AsyncScheduler(job_executors={"custom": executor})

    assert scheduler.task_defaults.job_executor == "custom"


def test_sync_default_executor_is_preserved_when_no_custom_mapping() -> None:
    """The blocking wrapper's historical threadpool default must not change."""
    scheduler = Scheduler()

    assert set(scheduler.job_executors) == {"async", "threadpool", "processpool"}
    assert scheduler.task_defaults.job_executor == "threadpool"


def test_explicit_task_default_is_not_overridden() -> None:
    """An explicitly chosen executor inside a custom mapping takes precedence."""
    from apscheduler import TaskDefaults

    scheduler = Scheduler(
        job_executors={"custom": AsyncJobExecutor()},
        task_defaults=TaskDefaults(job_executor="custom"),
    )

    assert scheduler.task_defaults.job_executor == "custom"
