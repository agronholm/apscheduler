from __future__ import annotations

import sys
from abc import ABCMeta, abstractmethod
from collections.abc import Iterable, Iterator, Sequence
from contextlib import AsyncExitStack
from datetime import datetime, timedelta
from logging import Logger
from typing import TYPE_CHECKING, Any, Callable
from uuid import UUID

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

if TYPE_CHECKING:
    from ._enums import ConflictPolicy
    from ._events import Event, T_Event
    from ._structures import Job, JobResult, Schedule, ScheduleResult, Task


class Trigger(Iterator[datetime], metaclass=ABCMeta):
    """
    Abstract base class that defines the interface that every trigger must implement.
    """

    __slots__ = ()

    @abstractmethod
    def next(self) -> datetime | None:
        """
        Return the next datetime to fire on.

        If no such datetime can be calculated, ``None`` is returned.

        :raises MaxIterationsReached: if the trigger's internal logic has exceeded a set
            maximum of iterations (used to detect potentially infinite loops)

        """

    @abstractmethod
    def __getstate__(self) -> Any:
        """Return the serializable state of the trigger."""

    @abstractmethod
    def __setstate__(self, state: Any) -> None:
        """Initialize an empty instance from an existing state."""

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> datetime:
        dateval = self.next()
        if dateval is None:
            raise StopIteration
        else:
            return dateval


class Serializer(metaclass=ABCMeta):
    """Interface for classes that implement (de)serialization."""

    __slots__ = ()

    @abstractmethod
    def serialize(self, obj: object) -> bytes:
        """
        Turn the given object into a bytestring.

        Must handle the serialization of at least any JSON type, plus the following:

        * ``datetime.date`` (using :meth:`datetime.date.isoformat`)
        * ``datetime.timedelta`` (using :meth:`datetime.timedelta.total_seconds`)
        * ``datetime.tzinfo`` (by extracting the time zone name)

        :return: a bytestring that can be later restored using :meth:`deserialize`
        """

    @abstractmethod
    def deserialize(self, serialized: bytes) -> Any:
        """
        Restore a previously serialized object from bytestring

        :param serialized: a bytestring previously received from :meth:`serialize`
        :return: a copy of the original object
        """


class Subscription(metaclass=ABCMeta):
    """
    Represents a subscription with an event source.

    If used as a context manager, unsubscribes on exit.
    """

    def __enter__(self) -> Subscription:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.unsubscribe()

    @abstractmethod
    def unsubscribe(self) -> None:
        """
        Cancel this subscription.

        Does nothing if the subscription has already been cancelled.
        """


class EventBroker(metaclass=ABCMeta):
    """
    Interface for objects that can be used to publish notifications to interested
    subscribers.
    """

    @abstractmethod
    async def start(self, exit_stack: AsyncExitStack, logger: Logger) -> None:
        """
        Start the event broker.

        :param exit_stack: an asynchronous exit stack which will be processed when the
            scheduler is shut down
        :param logger: the logger object the event broker should use to log events
        """

    @abstractmethod
    async def publish(self, event: Event) -> None:
        """Publish an event."""

    @abstractmethod
    async def publish_local(self, event: Event) -> None:
        """Publish an event, but only to local subscribers."""

    @abstractmethod
    def subscribe(
        self,
        callback: Callable[[T_Event], Any],
        event_types: Iterable[type[T_Event]] | None = None,
        *,
        is_async: bool = True,
        one_shot: bool = False,
    ) -> Subscription:
        """
        Subscribe to events from this event broker.

        :param callback: callable to be called with the event object when an event is
            published
        :param event_types: an iterable of concrete Event classes to subscribe to
        :param is_async: ``True`` if the (synchronous) callback should be called on the
            event loop thread, ``False`` if it should be called in a scheduler thread.
            If the callback is a coroutine function, this flag is ignored.
        :param one_shot: if ``True``, automatically unsubscribe after the first matching
            event
        """


class DataStore(metaclass=ABCMeta):
    """
    Interface for data stores.

    Data stores keep track of tasks, schedules and jobs. When these objects change, the
    data store publishes events to the associated event broker accordingly.
    """

    @abstractmethod
    async def start(
        self, exit_stack: AsyncExitStack, event_broker: EventBroker, logger: Logger
    ) -> None:
        """
        Start the event broker.

        :param exit_stack: an asynchronous exit stack which will be processed when the
            scheduler is shut down
        :param event_broker: the event broker shared between the scheduler, scheduler
            (if any) and this data store
        :param logger: the logger object the data store should use to log events
        """

    @abstractmethod
    async def add_task(self, task: Task) -> None:
        """
        Add the given task to the store.

        If a task with the same ID already exists, it replaces the old one but does NOT
        affect task accounting (# of running jobs).

        :param task: the task to be added
        """

    @abstractmethod
    async def remove_task(self, task_id: str) -> None:
        """
        Remove the task with the given ID.

        :param task_id: ID of the task to be removed
        :raises TaskLookupError: if no matching task was found
        """

    @abstractmethod
    async def get_task(self, task_id: str) -> Task:
        """
        Get an existing task definition.

        :param task_id: ID of the task to be returned
        :return: the matching task
        :raises TaskLookupError: if no matching task was found
        """

    @abstractmethod
    async def get_tasks(self) -> list[Task]:
        """
        Get all the tasks in this store.

        :return: a list of tasks, sorted by ID
        """

    @abstractmethod
    async def get_schedules(self, ids: set[str] | None = None) -> list[Schedule]:
        """
        Get schedules from the data store.

        :param ids: a specific set of schedule IDs to return, or ``None`` to return all
            schedules
        :return: the list of matching schedules, in unspecified order
        """

    @abstractmethod
    async def add_schedule(
        self, schedule: Schedule, conflict_policy: ConflictPolicy
    ) -> None:
        """
        Add or update the given schedule in the data store.

        :param schedule: schedule to be added
        :param conflict_policy: policy that determines what to do if there is an
            existing schedule with the same ID
        """

    @abstractmethod
    async def remove_schedules(self, ids: Iterable[str]) -> None:
        """
        Remove schedules from the data store.

        :param ids: a specific set of schedule IDs to remove
        """

    @abstractmethod
    async def acquire_schedules(
        self, scheduler_id: str, lease_duration: timedelta, limit: int
    ) -> list[Schedule]:
        """
        Acquire unclaimed due schedules for processing.

        This method claims up to the requested number of schedules for the given
        scheduler and returns them.

        For a stored schedule to be eligible for acquisition, it must fulfill one of the
        following conditions:

        * It is unclaimed (``acquired_until`` is ``None``)
        * Its claim has expired (``acquired_until`` is less than the current datetime)
        * It is claimed by the given scheduler (``acquired_by`` equals ``scheduler_id``)

        :param scheduler_id: unique identifier of the scheduler
        :param lease_duration: the duration of the lease, after which the schedules can be
            acquired by another scheduler even if ``acquired_by`` is not ``None``
        :param limit: maximum number of schedules to claim
        :return: the list of claimed schedules
        """

    @abstractmethod
    async def release_schedules(
        self, scheduler_id: str, results: Sequence[ScheduleResult]
    ) -> None:
        """
        Release the claims on the given schedules and update them on the store.

        The data store is responsible for updating the following fields on stored
        schedules:

        * ``last_fire_time``
        * ``next_fire_time``
        * ``trigger``
        * ``acquired_by`` (must beset to ``None``)
        * ``acquired_until`` (must be set to ``None``)

        :param scheduler_id: unique identifier of the scheduler
        :param results: list of schedule processing results
        """

    @abstractmethod
    async def get_next_schedule_run_time(self) -> datetime | None:
        """
        Return the earliest upcoming run time of all the schedules in the store, or
        ``None`` if there are no active schedules.
        """

    @abstractmethod
    async def add_job(self, job: Job) -> None:
        """
        Add a job to be executed by an eligible scheduler.

        :param job: the job object
        """

    @abstractmethod
    async def get_jobs(self, ids: Iterable[UUID] | None = None) -> list[Job]:
        """
        Get the list of pending jobs.

        :param ids: a specific set of job IDs to return, or ``None`` to return all jobs
        :return: the list of matching pending jobs, in the order they will be given to
            schedulers
        """

    @abstractmethod
    async def acquire_jobs(
        self, scheduler_id: str, lease_duration: timedelta, limit: int | None = None
    ) -> list[Job]:
        """
        Acquire unclaimed jobs for execution.

        This method claims up to the requested number of jobs for the given scheduler
        and returns them.

        :param scheduler_id: unique identifier of the scheduler
        :param lease_duration: the duration of the lease, after which the jobs will be
            considered to be dead if the scheduler doesn't extend the lease duration
        :param limit: maximum number of jobs to claim and return
        :return: the list of claimed jobs
        """

    @abstractmethod
    async def release_job(self, scheduler_id: str, job: Job, result: JobResult) -> None:
        """
        Release the claim on the given job and record the result.

        :param scheduler_id: unique identifier of the scheduler
        :param job: the job to be released
        :param result: the result of the job
        """

    @abstractmethod
    async def get_job_result(self, job_id: UUID) -> JobResult | None:
        """
        Retrieve the result of a job.

        The result is removed from the store after retrieval.

        :param job_id: the identifier of the job
        :return: the result, or ``None`` if the result was not found
        """

    async def extend_acquired_schedule_leases(
        self, scheduler_id: str, schedule_ids: set[str], duration: timedelta
    ) -> None:
        """
        Extend the leases of specified schedules acquired by the given scheduler.

        :param scheduler_id: unique identifier of the scheduler
        :param schedule_ids: the identifiers of the schedules the scheduler is currently
            processing
        :param duration: the duration by which to extend the leases
        """

    async def extend_acquired_job_leases(
        self, scheduler_id: str, job_ids: set[UUID], duration: timedelta
    ) -> None:
        """
        Extend the leases of specified jobs acquired by the given scheduler.

        :param scheduler_id: unique identifier of the scheduler
        :param job_ids: the identifiers of the jobs the scheduler is running
        :param duration: the duration by which to extend the leases
        """

    @abstractmethod
    async def reap_abandoned_jobs(self, scheduler_id: str) -> None:
        """
        Find jobs marked as acquired by the given scheduler ID and release them with the
        outcome of :attr:`~apscheduler.JobOutcome.abandoned`.

        Implementers must ensure that the proper :class:`~apscheduler.JobReleased`
        events are published.

        This method is called once during the scheduler startup sequence.

        :param scheduler_id: unique identifier of the scheduler
        """

    @abstractmethod
    async def cleanup(self) -> None:
        """
        Perform clean-up operations on the data store.

        This method must perform the following operations (in this order):

        * Purge expired job results (where ``expires_at`` is less or equal to the
          current time)
        * Release jobs with expired leases with the ``cancelled`` outcome
        * Purge finished schedules (where ``next_run_time`` is ``None``) that have no
          running jobs associated with them
        """


class JobExecutor(metaclass=ABCMeta):
    async def start(self, exit_stack: AsyncExitStack) -> None:
        """
        Start the job executor.

        :param exit_stack: an asynchronous exit stack which will be processed when the
            scheduler is shut down
        """

    @abstractmethod
    async def run_job(self, func: Callable[..., Any], job: Job) -> Any:
        """
        Run the given job by calling the given function.

        :param func: the function to call
        :param job: the associated job
        :return: the return value of ``func`` (potentially awaiting on the returned
            aawaitable, if any)
        """
