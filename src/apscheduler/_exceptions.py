from __future__ import annotations

from uuid import UUID


class TaskLookupError(LookupError):
    """Raised by a data store when it cannot find the requested task."""

    def __init__(self, task_id: str):
        super().__init__(f"No task by the id of {task_id!r} was found")


class ScheduleLookupError(LookupError):
    """Raised by a scheduler when it cannot find the requested schedule."""

    def __init__(self, schedule_id: str):
        super().__init__(f"No schedule by the id of {schedule_id!r} was found")


class JobLookupError(LookupError):
    """Raised when the job store cannot find a job for update or removal."""

    def __init__(self, job_id: UUID):
        super().__init__(f"No job by the id of {job_id} was found")


class JobResultNotReady(Exception):
    """
    Raised by :meth:`~.schedulers.sync.Scheduler.get_job_result` if the job result is
    not ready.
    """

    def __init__(self, job_id: UUID):
        super().__init__(f"No job by the id of {job_id} was found")


class JobCancelled(Exception):
    """
    Raised by :meth:`~.schedulers.sync.Scheduler.get_job_result` if the job was
    cancelled.
    """


class JobDeadlineMissed(Exception):
    """
    Raised by :meth:`~.schedulers.sync.Scheduler.get_job_result` if the job failed to
    start within the allotted time.
    """


class ConflictingIdError(KeyError):
    """
    Raised when trying to add a schedule to a store that already contains a schedule by
    that ID, and the conflict policy of ``exception`` is used.
    """

    def __init__(self, schedule_id):
        super().__init__(
            f"This data store already contains a schedule with the identifier "
            f"{schedule_id!r}"
        )


class SerializationError(Exception):
    """Raised when a serializer fails to serialize the given object."""


class DeserializationError(Exception):
    """Raised when a serializer fails to deserialize the given object."""


class MaxIterationsReached(Exception):
    """
    Raised when a trigger has reached its maximum number of allowed computation
    iterations when trying to calculate the next fire time.
    """
