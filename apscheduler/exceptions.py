

class JobLookupError(KeyError):
    """Raised when the job store cannot find a job for update or removal."""

    def __init__(self, job_id):
        super().__init__(u'No job by the id of %s was found' % job_id)


class ConflictingIdError(KeyError):
    """Raised when the uniqueness of job IDs is being violated."""

    def __init__(self, job_id):
        super().__init__(
            u'Job identifier (%s) conflicts with an existing job' % job_id)


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
