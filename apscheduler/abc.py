from abc import ABCMeta, abstractmethod
from base64 import b64decode, b64encode
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import (
    Any, Callable, Dict, FrozenSet, Iterable, Iterator, List, Optional, Set, Type)
from uuid import UUID, uuid4

from .policies import CoalescePolicy, ConflictPolicy


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


@dataclass
class Task:
    id: str
    func: Callable = field(compare=False)
    max_instances: Optional[int] = field(compare=False, default=None)
    metadata_arg: Optional[str] = field(compare=False, default=None)
    stateful: bool = field(compare=False, default=False)
    misfire_grace_time: Optional[timedelta] = field(compare=False, default=None)


@dataclass(unsafe_hash=True)
class Schedule:
    id: str
    task_id: str = field(compare=False)
    trigger: Trigger = field(compare=False)
    args: tuple = field(compare=False)
    kwargs: Dict[str, Any] = field(compare=False)
    coalesce: CoalescePolicy = field(compare=False)
    misfire_grace_time: Optional[timedelta] = field(compare=False)
    tags: FrozenSet[str] = field(compare=False)
    next_fire_time: Optional[datetime] = field(compare=False, default=None)
    last_fire_time: Optional[datetime] = field(init=False, compare=False, default=None)

    @property
    def next_deadline(self) -> Optional[datetime]:
        if self.next_fire_time and self.misfire_grace_time:
            return self.next_fire_time + self.misfire_grace_time

        return None


@dataclass(unsafe_hash=True)
class Job:
    id: UUID = field(init=False, default_factory=uuid4)
    task_id: str = field(compare=False)
    func: Callable = field(compare=False)
    args: tuple = field(compare=False)
    kwargs: Dict[str, Any] = field(compare=False)
    schedule_id: Optional[str] = field(compare=False, default=None)
    scheduled_fire_time: Optional[datetime] = field(compare=False, default=None)
    start_deadline: Optional[datetime] = field(compare=False, default=None)
    tags: Optional[FrozenSet[str]] = field(compare=False, default_factory=frozenset)
    started_at: Optional[datetime] = field(init=False, compare=False, default=None)


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


@dataclass(frozen=True)
class Event:
    timestamp: datetime


@dataclass
class EventSource(metaclass=ABCMeta):
    @abstractmethod
    def subscribe(self, callback: Callable[[Event], Any],
                  event_types: Optional[Iterable[Type[Event]]] = None) -> None:
        """
        Subscribe to events from this event source.

        :param callback: callable to be called with the event object when an event is published
        :param event_types: an iterable of concrete Event classes to subscribe to
        """

    @abstractmethod
    def unsubscribe(self, callback: Callable[[Event], Any],
                    event_types: Optional[Iterable[Type[Event]]] = None) -> None:
        """
        Cancel an event subscription

        :param callback:
        :param event_types: an iterable of concrete Event classes to unsubscribe from
        :return:
        """

    @abstractmethod
    async def publish(self, event: Event) -> None:
        """
        Publish an event.

        :param event: the event to publish
        """


class DataStore(EventSource):
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
