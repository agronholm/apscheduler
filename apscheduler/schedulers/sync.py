from __future__ import annotations

from concurrent.futures import Future
from functools import partial
from logging import Logger
from typing import Any, Callable, Iterable, Mapping, Optional, Union

from anyio.abc import BlockingPortal

from .async_ import AsyncScheduler
from ..abc import DataStore, Trigger
from ..events import SyncEventSource
from ..workers.sync import SyncWorker


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

    # def __exit__(self, exc_type, exc_val, exc_tb):
    #     return self._exit_stack.__exit__(exc_type, exc_val, exc_tb)

    # def __enter__(self) -> SyncScheduler:
    #     self.start()
    #     return self
    #
    # def __exit__(self, exc_type, exc_val, exc_tb):
    #     self.stop(force=exc_type is not None)

    # @property
    # def worker(self) -> AsyncWorker:
    #     return self._scheduler.worker

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

    # def start(self):
    #     if not self._scheduler._running:
    #         if not self.portal:
    #             self.portal = start_blocking_portal()
    #             self._shutdown_portal = True
    #
    #         self.portal.call(self._scheduler.data_store.initialize)
    #         start_event = self.portal.call(create_event)
    #         self._task = self.portal.spawn_task(self._scheduler.run, start_event)
    #         self.portal.call(start_event.wait)
    #         print('start_event wait finished')
    #
    #         if self._scheduler.start_worker:
    #             self._worker = SyncWorker(self.data_store, portal=self.portal)
    #             self._worker.start()
    #
    # def stop(self, force: bool = False) -> None:
    #     if self._worker:
    #         self._worker.stop(force)
    #
    #     if self._scheduler._running:
    #         self._scheduler._running = False
    #         try:
    #             self.portal.call(partial(self._scheduler.stop, force=force))
    #         finally:
    #             if self._shutdown_portal:
    #                 self.portal.stop_from_external_thread(cancel_remaining=force)
