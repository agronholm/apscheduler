from __future__ import annotations

from concurrent.futures import Future
from functools import partial
from logging import Logger
from typing import Any, Callable, Iterable, Mapping, Optional, Union

from anyio.abc import BlockingPortal

from ..abc import DataStore, Trigger
from ..events import SyncEventSource
from ..workers.sync import SyncWorker
from .async_ import AsyncScheduler


class SyncScheduler(SyncEventSource):
    """
    A synchronous scheduler implementation.

    Wraps an :class:`~.async_.AsyncScheduler` using a :class:`~anyio.abc.BlockingPortal`.

    :ivar BlockingPortal portal: the blocking portal used by the scheduler
    """

    _task: Future
    _worker: Optional[SyncWorker] = None

    def __init__(self, data_store: Optional[DataStore] = None, *, identity: Optional[str] = None,
                 logger: Optional[Logger] = None, start_worker: bool = True,
                 portal: Optional[BlockingPortal] = None):
        self._scheduler = AsyncScheduler(data_store, identity=identity, logger=logger,
                                         start_worker=False)
        self.start_worker = start_worker
        super().__init__(self._scheduler, portal)

    def __enter__(self):
        super().__enter__()
        self._exit_stack.enter_context(self.portal.wrap_async_context_manager(self._scheduler))

        if self.start_worker:
            self._worker = SyncWorker(self.data_store, portal=self.portal)
            self._exit_stack.enter_context(self.worker)

        return self

    @property
    def data_store(self) -> DataStore:
        return self._scheduler.data_store

    @property
    def worker(self) -> Optional[SyncWorker]:
        return self._worker

    def add_schedule(self, task: Union[str, Callable], trigger: Trigger, *,
                     args: Optional[Iterable] = None,
                     kwargs: Optional[Mapping[str, Any]] = None) -> str:
        func = partial(self._scheduler.add_schedule, task, trigger, args=args, kwargs=kwargs)
        return self.portal.call(func)

    def remove_schedule(self, schedule_id: str) -> None:
        self.portal.call(self._scheduler.remove_schedule, schedule_id)

    def stop(self) -> None:
        self.portal.call(self._scheduler.stop)

    def wait_until_stopped(self) -> None:
        self.portal.call(self._scheduler.wait_until_stopped)
