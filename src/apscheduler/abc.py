from __future__ import annotations

from abc import ABCMeta, abstractmethod
from base64 import b64decode, b64encode
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Iterable, Iterator, List, Optional, Set, Type
from uuid import UUID

from .policies import ConflictPolicy
from .structures import Job, Schedule

if TYPE_CHECKING:
    from . import events


class Trigger(Iterator[datetime], metaclass=ABCMeta):
    """Abstract base class that defines the interface that every trigger must implement."""

    __slots__ = ()

    @abstractmethod
    def next(self) -> Optional[datetime]:
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
    __slots__ = ()

    @abstractmethod
    def serialize(self, obj) -> bytes:
        pass

    def serialize_to_unicode(self, obj) -> str:
        return b64encode(self.serialize(obj)).decode('ascii')

    @abstractmethod
    def deserialize(self, serialized: bytes):
        pass

    def deserialize_from_unicode(self, serialized: str):
        return self.deserialize(b64decode(serialized))


class EventSource(metaclass=ABCMeta):
    @abstractmethod
    def subscribe(
        self, callback: Callable[[events.Event], Any],
        event_types: Optional[Iterable[Type[events.Event]]] = None
    ) -> events.SubscriptionToken:
        """
        Subscribe to events from this event source.

        :param callback: callable to be called with the event object when an event is published
        :param event_types: an iterable of concrete Event classes to subscribe to
        """

    @abstractmethod
    def unsubscribe(self, token: events.SubscriptionToken) -> None:
        """
        Cancel an event subscription.

        :param token: a token returned from :meth:`subscribe`
        """


class DataStore(EventSource):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @abstractmethod
    def get_schedules(self, ids: Optional[Set[str]] = None) -> List[Schedule]:
        """
        Get schedules from the data store.

        :param ids: a specific set of schedule IDs to return, or ``None`` to return all schedules
        :return: the list of matching schedules, in unspecified order
        """

    @abstractmethod
    def add_schedule(self, schedule: Schedule, conflict_policy: ConflictPolicy) -> None:
        """
        Add or update the given schedule in the data store.

        :param schedule: schedule to be added
        :param conflict_policy: policy that determines what to do if there is an existing schedule
            with the same ID
        """

    @abstractmethod
    def remove_schedules(self, ids: Iterable[str]) -> None:
        """
        Remove schedules from the data store.

        :param ids: a specific set of schedule IDs to remove
        """

    @abstractmethod
    def acquire_schedules(self, scheduler_id: str, limit: int) -> List[Schedule]:
        """
        Acquire unclaimed due schedules for processing.

        This method claims up to the requested number of schedules for the given scheduler and
        returns them.

        :param scheduler_id: unique identifier of the scheduler
        :param limit: maximum number of schedules to claim
        :return: the list of claimed schedules
        """

    @abstractmethod
    def release_schedules(self, scheduler_id: str, schedules: List[Schedule]) -> None:
        """
        Release the claims on the given schedules and update them on the store.

        :param scheduler_id: unique identifier of the scheduler
        :param schedules: the previously claimed schedules
        """

    @abstractmethod
    def add_job(self, job: Job) -> None:
        """
        Add a job to be executed by an eligible worker.

        :param job: the job object
        """

    @abstractmethod
    def get_jobs(self, ids: Optional[Iterable[UUID]] = None) -> List[Job]:
        """
        Get the list of pending jobs.

        :param ids: a specific set of job IDs to return, or ``None`` to return all jobs
        :return: the list of matching pending jobs, in the order they will be given to workers
        """

    @abstractmethod
    def acquire_jobs(self, worker_id: str, limit: Optional[int] = None) -> List[Job]:
        """
        Acquire unclaimed jobs for execution.

        This method claims up to the requested number of jobs for the given worker and returns
        them.

        :param worker_id: unique identifier of the worker
        :param limit: maximum number of jobs to claim and return
        :return: the list of claimed jobs
        """

    @abstractmethod
    def release_jobs(self, worker_id: str, jobs: List[Job]) -> None:
        """
        Releases the claim on the given jobs

        :param worker_id: unique identifier of the worker
        :param jobs: the previously claimed jobs
        """


class AsyncDataStore(EventSource):
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    @abstractmethod
    async def get_schedules(self, ids: Optional[Set[str]] = None) -> List[Schedule]:
        """
        Get schedules from the data store.

        :param ids: a specific set of schedule IDs to return, or ``None`` to return all schedules
        :return: the list of matching schedules, in unspecified order
        """

    @abstractmethod
    async def add_schedule(self, schedule: Schedule, conflict_policy: ConflictPolicy) -> None:
        """
        Add or update the given schedule in the data store.

        :param schedule: schedule to be added
        :param conflict_policy: policy that determines what to do if there is an existing schedule
            with the same ID
        """

    @abstractmethod
    async def remove_schedules(self, ids: Iterable[str]) -> None:
        """
        Remove schedules from the data store.

        :param ids: a specific set of schedule IDs to remove
        """

    @abstractmethod
    async def acquire_schedules(self, scheduler_id: str, limit: int) -> List[Schedule]:
        """
        Acquire unclaimed due schedules for processing.

        This method claims up to the requested number of schedules for the given scheduler and
        returns them.

        :param scheduler_id: unique identifier of the scheduler
        :param limit: maximum number of schedules to claim
        :return: the list of claimed schedules
        """

    @abstractmethod
    async def release_schedules(self, scheduler_id: str, schedules: List[Schedule]) -> None:
        """
        Release the claims on the given schedules and update them on the store.

        :param scheduler_id: unique identifier of the scheduler
        :param schedules: the previously claimed schedules
        """

    @abstractmethod
    async def add_job(self, job: Job) -> None:
        """
        Add a job to be executed by an eligible worker.

        :param job: the job object
        """

    @abstractmethod
    async def get_jobs(self, ids: Optional[Iterable[UUID]] = None) -> List[Job]:
        """
        Get the list of pending jobs.

        :param ids: a specific set of job IDs to return, or ``None`` to return all jobs
        :return: the list of matching pending jobs, in the order they will be given to workers
        """

    @abstractmethod
    async def acquire_jobs(self, worker_id: str, limit: Optional[int] = None) -> List[Job]:
        """
        Acquire unclaimed jobs for execution.

        This method claims up to the requested number of jobs for the given worker and returns
        them.

        :param worker_id: unique identifier of the worker
        :param limit: maximum number of jobs to claim and return
        :return: the list of claimed jobs
        """

    @abstractmethod
    async def release_jobs(self, worker_id: str, jobs: List[Job]) -> None:
        """
        Releases the claim on the given jobs

        :param worker_id: unique identifier of the worker
        :param jobs: the previously claimed jobs
        """
