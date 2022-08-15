from __future__ import annotations

from enum import Enum, auto


class RunState(Enum):
    """
    Used to track the running state of schedulers and workers.

    Values:

    * ``starting``: not running yet, but in the process of starting
    * ``started``: running
    * ``stopping``: still running but in the process of shutting down
    * ``stopped``: not running
    """

    starting = auto()
    started = auto()
    stopping = auto()
    stopped = auto()


class JobOutcome(Enum):
    """
    Used to indicate how the execution of a job ended.

    Values:

    * ``success``: the job completed successfully
    * ``error``: the job raised an exception
    * ``missed_start_deadline``: the job's execution was delayed enough for it to miss
      its deadline
    * ``cancelled``: the job's execution was cancelled
    """

    success = auto()
    error = auto()
    missed_start_deadline = auto()
    cancelled = auto()


class ConflictPolicy(Enum):
    """
    Used to indicate what to do when trying to add a schedule whose ID conflicts with an
    existing schedule.

    Values:

    * ``replace``: replace the existing schedule with a new one
    * ``do_nothing``: keep the existing schedule as-is and drop the new schedule
    * ``exception``: raise an exception if a conflict is detected
    """

    replace = auto()
    do_nothing = auto()
    exception = auto()


class CoalescePolicy(Enum):
    """
    Used to indicate how to queue jobs for a schedule that has accumulated multiple
    run times since the last scheduler iteration.

    Values:

    * ``earliest``: run once, with the earliest fire time
    * ``latest``: run once, with the latest fire time
    * ``all``: submit one job for every accumulated fire time
    """

    earliest = auto()
    latest = auto()
    all = auto()
