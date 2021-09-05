class TaskLookupError(LookupError):
    """Raised by a data store when it cannot find the requested task."""

    def __init__(self, task_id: str):
        super().__init__(f'No task by the id of {task_id!r} was found')


class JobLookupError(KeyError):
    """Raised when the job store cannot find a job for update or removal."""

    def __init__(self, job_id):
        super().__init__(u'No job by the id of %s was found' % job_id)


class ConflictingIdError(KeyError):
    """
    Raised when trying to add a schedule to a store that already contains a schedule by that ID,
    and the conflict policy of ``exception`` is used.
    """

    def __init__(self, schedule_id):
        super().__init__(
            f'This data store already contains a schedule with the identifier {schedule_id!r}')


class TransientJobError(ValueError):
    """
    Raised when an attempt to add transient (with no func_ref) job to a persistent job store is
    detected.
    """

    def __init__(self, job_id):
        super().__init__(
            f'Job ({job_id}) cannot be added to this job store because a reference to the '
            f'callable could not be determined.')


class SerializationError(Exception):
    """Raised when a serializer fails to serialize the given object."""


class DeserializationError(Exception):
    """Raised when a serializer fails to deserialize the given object."""


class MaxIterationsReached(Exception):
    """
    Raised when a trigger has reached its maximum number of allowed computation iterations when
    trying to calculate the next fire time.
    """


class SchedulerAlreadyRunningError(Exception):
    """Raised when attempting to start or configure the scheduler when it's already running."""

    def __str__(self):
        return 'Scheduler is already running'


class SchedulerNotRunningError(Exception):
    """Raised when attempting to shutdown the scheduler when it's not running."""

    def __str__(self):
        return 'Scheduler is not running'
