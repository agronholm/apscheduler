from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
from typing import Any, TypeVar

import attrs

from ._structures import TaskDefaults
from ._utils import UnsetValue, unset

T = TypeVar("T", bound="Callable[..., Any]")

TASK_PARAMETERS_KEY = "_apscheduler_taskdef"


@attrs.define(kw_only=True)
class TaskParameters(TaskDefaults):
    id: str | UnsetValue = attrs.field(default=unset)


def task(
    id: str | UnsetValue = unset,
    *,
    job_executor: str | UnsetValue = unset,
    max_running_jobs: int | None | UnsetValue = unset,
    misfire_grace_time: int | timedelta | None | UnsetValue = unset,
) -> Callable[[T], T]:
    """
    Decorate a function to have implied defaults as an APScheduler task.

    :param id: the task ID to use
    :param str job_executor: name of the job executor that will run the task
    :param int | None max_running_jobs: maximum number of instances of the task that are
        allowed to run concurrently
    :param ~datetime.timedelta | None misfire_grace_time: maximum number of seconds the
        run time of jobs created for the task are allowed to be late, compared to the
        scheduled run time

    """

    def wrapper(func: T) -> T:
        if not isinstance(func, Callable):
            raise ValueError("only functions can be decorated with @task")

        if hasattr(func, TASK_PARAMETERS_KEY):
            raise ValueError(
                "this function already has APScheduler task parameters set"
            )

        setattr(
            func,
            TASK_PARAMETERS_KEY,
            TaskParameters(
                id=id,
                job_executor=job_executor,
                max_running_jobs=max_running_jobs,
                misfire_grace_time=misfire_grace_time,
            ),
        )
        return func

    return wrapper


def get_task_params(func: Callable[..., Any]) -> TaskParameters:
    return getattr(func, TASK_PARAMETERS_KEY, None) or TaskParameters()
