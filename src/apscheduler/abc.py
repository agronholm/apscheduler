from __future__ import annotations

from abc import ABCMeta, abstractmethod
from contextlib import AsyncExitStack
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Iterable, Iterator
from uuid import UUID

if TYPE_CHECKING:
    from ._enums import ConflictPolicy
    from ._events import Event
    from ._structures import Job, JobResult, Schedule, Task


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
        :raises apscheduler.exceptions.MaxIterationsReached:
        """

    @abstractmethod
    def __getstate__(self):
        """Return the (JSON compatible) serializable state of the trigger."""

    @abstractmethod
    def __setstate__(self, state):
        """Initialize an empty instance from an existing state."""

    def __iter__(self):
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
    def serialize(self, obj: Any) -> bytes:
        """
        Turn the given object into a bytestring.

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
    async def start(self, exit_stack: AsyncExitStack) -> None:
        """
        Start the event broker.

        :param exit_stack: an asynchronous exit stack which will be processed when the
            scheduler is shut down
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
        callback: Callable[[Event], Any],
        event_types: Iterable[type[Event]] | None = None,
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
            event loop thread, ``False`` if it should be called in a worker thread.
            If the callback is a coroutine function, this flag is ignored.
        :param one_shot: if ``True``, automatically unsubscribe after the first matching
            event
        """


class DataStore(metaclass=ABCMeta):
    """Asynchronous version of :class:`DataStore`. Expected to work on asyncio."""

    @abstractmethod
    async def start(
        self, exit_stack: AsyncExitStack, event_broker: EventBroker
    ) -> None:
        """
        Start the event broker.

        :param exit_stack: an asynchronous exit stack which will be processed when the
            scheduler is shut down
        :param event_broker: the event broker shared between the scheduler, worker (if
            any) and this data store
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
    async def acquire_schedules(self, scheduler_id: str, limit: int) -> list[Schedule]:
        """
        Acquire unclaimed due schedules for processing.

        This method claims up to the requested number of schedules for the given
        scheduler and returns them.

        :param scheduler_id: unique identifier of the scheduler
        :param limit: maximum number of schedules to claim
        :return: the list of claimed schedules
        """

    @abstractmethod
    async def release_schedules(
        self, scheduler_id: str, schedules: list[Schedule]
    ) -> None:
        """
        Release the claims on the given schedules and update them on the store.

        :param scheduler_id: unique identifier of the scheduler
        :param schedules: the previously claimed schedules
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
        Add a job to be executed by an eligible worker.

        :param job: the job object
        """

    @abstractmethod
    async def get_jobs(self, ids: Iterable[UUID] | None = None) -> list[Job]:
        """
        Get the list of pending jobs.

        :param ids: a specific set of job IDs to return, or ``None`` to return all jobs
        :return: the list of matching pending jobs, in the order they will be given to
            workers
        """

    @abstractmethod
    async def acquire_jobs(self, worker_id: str, limit: int | None = None) -> list[Job]:
        """
        Acquire unclaimed jobs for execution.

        This method claims up to the requested number of jobs for the given worker and
        returns them.

        :param worker_id: unique identifier of the worker
        :param limit: maximum number of jobs to claim and return
        :return: the list of claimed jobs
        """

    @abstractmethod
    async def release_job(
        self, worker_id: str, task_id: str, result: JobResult
    ) -> None:
        """
        Release the claim on the given job and record the result.

        :param worker_id: unique identifier of the worker
        :param task_id: the job's task ID
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

        :param func:
        :param job:
        :return: the return value of ``func`` (potentially awaiting on the returned
            aawaitable, if any)
        """
