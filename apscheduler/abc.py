from abc import ABCMeta, abstractmethod
from base64 import b64encode, b64decode
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import (
    Callable, Iterable, Iterator, Mapping, Any, NoReturn, Optional, Union, AsyncIterable, Dict,
    FrozenSet, List, Set)

from .events import Event


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
    func: Callable
    max_instances: Optional[int] = None
    metadata_arg: Optional[str] = None
    stateful: bool = False
    misfire_grace_time: Optional[timedelta] = None


@dataclass
class Schedule:
    id: str
    task_id: str
    trigger: Trigger
    args: tuple = ()
    kwargs: Dict[str, Any] = field(default_factory=dict)
    coalesce: bool = True
    misfire_grace_time: Optional[timedelta] = None
    tags: Optional[FrozenSet[str]] = frozenset()
    last_fire_time: Optional[datetime] = field(init=False, default=None)
    next_fire_time: Optional[datetime] = field(init=False, default=None)


@dataclass(frozen=True)
class Job:
    func_ref: str
    args: Optional[tuple] = None
    kwargs: Optional[Dict[str, Any]] = None
    schedule_id: Optional[str] = None
    scheduled_start_time: Optional[datetime] = None
    start_deadline: Optional[datetime] = None
    tags: Optional[FrozenSet[str]] = frozenset()


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


class DataStore(metaclass=ABCMeta):
    __slots__ = ()

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self):
        await self.stop()

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    @abstractmethod
    async def add_or_update_schedule(self, schedule: Schedule) -> None:
        """Add or update the given schedule in the store."""

    @abstractmethod
    async def remove_schedule(self, schedule_id: str) -> None:
        """Remove the designated schedule from the store."""

    @abstractmethod
    async def remove_all_schedules(self) -> None:
        """Remove all schedules from the store."""

    @abstractmethod
    async def get_all_schedules(self) -> List[Schedule]:
        """Get a list of all schedules, sorted on the "id" attribute."""

    @abstractmethod
    async def get_next_fire_time(self) -> Optional[datetime]:
        """
        Return the earliest fire time among all unclaimed schedules.

        If no running, unclaimed schedules exist, ``None`` is returned.
        """

    @abstractmethod
    async def acquire_due_schedules(self, scheduler_id: str) -> List[Schedule]:
        """
        Acquire an undefined amount of due schedules not claimed by any other scheduler.

        This method claims due schedules for the given scheduler and returns them.
        When the scheduler has updated the objects, it calls :meth:`release_due_schedules` to
        release the claim on them.
        """

    @abstractmethod
    async def release_due_schedules(self, scheduler_id: str, schedules: List[Schedule]) -> None:
        """
        Update the given schedules and release the claim on them held by this scheduler.

        This method should do the following:

        #. Remove any of the schedules in the store that have no next fire time
        #. Update the schedules that do have a next fire time
        #. Release any locks held on the schedules by this scheduler

        :param scheduler_id: identifier of the scheduler
        :param schedules: schedules previously acquired using :meth:`acquire_due_schedules`
        """

    @abstractmethod
    async def acquire_job(self, worker_id: str, tags: Set[str]) -> Job:
        """
        Claim and return the next matching job from the queue.

        :return: the acquired job
        """

    @abstractmethod
    async def release_job(self, job: Job) -> None:
        """Remove the given job from the queue."""


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
    async def subscribe(self) -> AsyncIterable[Event]:
        """Return an asynchronous iterable yielding newly received events."""


class AsyncScheduler(metaclass=ABCMeta):
    __slots__ = ()

    @abstractmethod
    def define_task(self, func: Callable, task_id: Optional[str] = None, *,
                    max_instances: Optional[int],
                    misfire_grace_time: Union[float, timedelta]) -> str:
        if not task_id:
            task_id = f'{func.__module__}.{func.__qualname__}'
        if isinstance(misfire_grace_time, float):
            misfire_grace_time = timedelta(misfire_grace_time)

        task = Task(id=task_id, func=func, max_instances=max_instances,
                    misfire_grace_time=misfire_grace_time)

        return task_id

    @abstractmethod
    async def add_schedule(self, task: Union[str, Callable], trigger: Trigger, *, args: Iterable,
                           kwargs: Mapping[str, Any]) -> str:
        """



        :param task: callable or ID of a predefined task
        :param trigger: trigger to define the run times of the schedule
        :param args: positional arguments to pass to the task callable
        :param kwargs: keyword arguments to pass to the task callable
        :return: identifier of the created schedule
        """

    @abstractmethod
    async def remove_schedule(self, schedule_id: str) -> None:
        """Removes the designated schedule."""

    @abstractmethod
    async def run(self) -> NoReturn:
        """
        Runs the scheduler loop.

        This method does not return.
        """


class SyncScheduler(metaclass=ABCMeta):
    __slots__ = ()

    @abstractmethod
    def add_schedule(self, task: Callable, trigger: Trigger, *, args: Iterable,
                     kwargs: Mapping[str, Any]) -> str:
        pass

    @abstractmethod
    def remove_schedule(self, schedule_id: str) -> None:
        pass

    @abstractmethod
    def run(self) -> NoReturn:
        pass

    add_schedule.__doc__ = AsyncScheduler.add_schedule.__doc__
    remove_schedule.__doc__ = AsyncScheduler.remove_schedule.__doc__
    run.__doc__ = AsyncScheduler.run.__doc__
