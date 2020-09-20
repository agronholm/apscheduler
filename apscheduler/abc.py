from abc import ABCMeta, abstractmethod
from base64 import b64decode, b64encode
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import (
    Any, AsyncContextManager, Callable, Dict, FrozenSet, Iterable, Iterator, List, Mapping,
    Optional, Set)
from uuid import uuid4

from apscheduler.events import Event


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
    coalesce: bool = field(compare=False)
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
    id: str = field(init=False, default_factory=lambda: str(uuid4()))
    task_id: str = field(compare=False)
    func: Callable = field(compare=False)
    args: tuple = field(compare=False)
    kwargs: Dict[str, Any] = field(compare=False)
    schedule_id: Optional[str] = field(compare=False, default=None)
    scheduled_start_time: Optional[datetime] = field(compare=False, default=None)
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


class EventSource(metaclass=ABCMeta):
    __slots__ = ()

    @abstractmethod
    async def subscribe(self, callback: Callable[[Event], Any]) -> None:
        """
        Subscribe to events from this event source.

        :param callback: callable to be called with the event object when an event is published
        :return: an async iterable yielding event objects
        """


class DataStore(EventSource):
    __slots__ = ()

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
    async def add_or_replace_schedules(self, schedules: Iterable[Schedule]) -> None:
        """
        Add or update the given schedule in the data store.

        :param schedules: schedules to be added or updated
        """

    @abstractmethod
    async def update_schedules(self, updates: Mapping[str, Dict[str, Any]]) -> Set[str]:
        """
        Update one or more existing schedules.

        :param updates: mapping of schedule ID to attribute names to be updated
        :return: the set of schedule IDs that were modified by this operation.
        """

    @abstractmethod
    async def remove_schedules(self, ids: Optional[Set[str]] = None) -> None:
        """
        Remove schedules from the data store.

        :param ids: a specific set of schedule IDs to remove, or ``None`` in which case all
            schedules are removed
        """

    @abstractmethod
    async def get_next_fire_time(self) -> Optional[datetime]:
        """
        Return the earliest fire time among all unclaimed schedules.

        If no running, unclaimed schedules exist, ``None`` is returned.
        """

    @abstractmethod
    async def acquire_due_schedules(
            self, scheduler_id: str,
            max_scheduled_time: datetime) -> AsyncContextManager[List[Schedule]]:
        """
        Acquire an undefined amount of due schedules not claimed by any other scheduler.

        This method claims due schedules for the given scheduler and returns them.
        When the scheduler has updated the objects, it calls :meth:`release_due_schedules` to
        release the claim on them.
        """

    # @abstractmethod
    # async def release_due_schedules(self, scheduler_id: str, schedules: List[Schedule]) -> None:
    #     """
    #     Update the given schedules and release the claim on them held by this scheduler.
    #
    #     This method should do the following:
    #
    #     #. Remove any of the schedules in the store that have no next fire time
    #     #. Update the schedules that do have a next fire time
    #     #. Release any locks held on the schedules by this scheduler
    #
    #     :param scheduler_id: identifier of the scheduler
    #     :param schedules: schedules previously acquired using :meth:`acquire_due_schedules`
    #     """

    # @abstractmethod
    # async def acquire_job(self, worker_id: str, tags: Set[str]) -> Job:
    #     """
    #     Claim and return the next matching job from the queue.
    #
    #     :return: the acquired job
    #     """
    #
    # @abstractmethod
    # async def release_job(self, job: Job) -> None:
    #     """Remove the given job from the queue."""


class Executor(EventSource):
    @abstractmethod
    async def submit_job(self, job: Job) -> None:
        """
        Submit a task to be run in this executor.

        The executor may alter the ``id`` attribute of the job before returning.

        :param job: the job object
        """

    @abstractmethod
    async def get_jobs(self) -> List[Job]:
        """
        Get jobs currently queued or running in this executor.

        :return: list of jobs
        """


class EventHub(metaclass=ABCMeta):
    __slots__ = ()

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    @abstractmethod
    async def publish(self, event: Event) -> None:
        """Publish an event."""

    @abstractmethod
    async def subscribe(self, callback: Callable[[Event], Any]) -> None:
        """Add a callback to be called when a new event is published."""
