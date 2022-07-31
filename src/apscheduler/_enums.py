from __future__ import annotations

from enum import Enum, auto


class RunState(Enum):
    """Used to track the running state of schedulers and workers."""

    #: not running yet, but in the process of starting
    starting = auto()
    #: running
    started = auto()
    #: still running but in the process of shutting down
    stopping = auto()
    #: not running
    stopped = auto()


class JobOutcome(Enum):
    """Used to indicate how the execution of a job ended."""

    #: the job completed successfully
    success = auto()
    #: the job raised an exception
    error = auto()
    #: the job's execution was delayed enough for it to miss its designated run time by
    #: too large a margin
    missed_start_deadline = auto()
    #: the job's execution was cancelled
    cancelled = auto()


class ConflictPolicy(Enum):
    """
    Used to indicate what to do when trying to add a schedule whose ID conflicts with an
    existing schedule.
    """

    #: replace the existing schedule with a new one
    replace = auto()
    #: keep the existing schedule as-is and drop the new schedule
    do_nothing = auto()
    #: raise an exception if a conflict is detected
    exception = auto()


class CoalescePolicy(Enum):
    """
    Used to indicate how to
    """

    #: run once, with the earliest fire time
    earliest = auto()
    #: run once, with the latest fire time
    latest = auto()
    #: submit one job for every accumulated fire time
    all = auto()
