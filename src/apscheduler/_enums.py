from __future__ import annotations

from enum import Enum, auto


class SchedulerRole(Enum):
    """
    Specifies what the scheduler should be doing when it's running.

    .. attribute:: scheduler

        processes due schedules, but won't run jobs

    .. attribute:: worker

        runs due jobs, but won't process schedules

    .. attribute:: both

        processes schedules and runs due jobs
    """

    scheduler = auto()
    worker = auto()
    both = auto()


class RunState(Enum):
    """
    Used to track the running state of schedulers.

    .. attribute:: starting

        not running yet, but in the process of starting

    .. attribute:: started

        running

    .. attribute:: stopping

        still running but in the process of shutting down

    .. attribute:: stopped

        not running
    """

    starting = auto()
    started = auto()
    stopping = auto()
    stopped = auto()


class JobOutcome(Enum):
    """
    Used to indicate how the execution of a job ended.

    .. attribute:: success

        the job completed successfully

    .. attribute:: error

        the job raised an exception

    .. attribute:: missed_start_deadline

        the job's execution was delayed enough for it to miss its start deadline
        (scheduled time + misfire grace time)

    .. attribute:: deserialization_failed

        the deserialization operation failed

    .. attribute:: cancelled

        the job's execution was cancelled

    .. attribute:: abandoned

        the worker running the job stopped unexpectedly and the job was never marked
        as done
    """

    success = auto()
    error = auto()
    missed_start_deadline = auto()
    deserialization_failed = auto()
    cancelled = auto()
    abandoned = auto()


class ConflictPolicy(Enum):
    """
    Used to indicate what to do when trying to add a schedule whose ID conflicts with an
    existing schedule.

    .. attribute:: replace

        replace the existing schedule with a new one

    .. attribute:: do_nothing

        keep the existing schedule as-is and drop the new schedule

    .. attribute:: exception

        raise an exception if a conflict is detected
    """

    replace = auto()
    do_nothing = auto()
    exception = auto()


class CoalescePolicy(Enum):
    """
    Used to indicate how to queue jobs for a schedule that has accumulated multiple
    run times since the last scheduler iteration.

    .. attribute:: earliest

        run once, with the earliest fire time

    .. attribute:: latest

        run once, with the latest fire time

    .. attribute:: all

        submit one job for every accumulated fire time
    """

    earliest = auto()
    latest = auto()
    all = auto()
