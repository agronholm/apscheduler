from __future__ import annotations

from typing import Optional

from anyio.abc import BlockingPortal

from ..abc import DataStore
from ..events import SyncEventSource
from .async_ import AsyncWorker


class SyncWorker(SyncEventSource):
    def __init__(self, data_store: DataStore, *, portal: Optional[BlockingPortal] = None,
                 max_concurrent_jobs: int = 100, identity: Optional[str] = None):
        self._worker = AsyncWorker(data_store, max_concurrent_jobs=max_concurrent_jobs,
                                   identity=identity, run_sync_functions_in_event_loop=False)
        super().__init__(self._worker, portal)

    def __enter__(self) -> SyncWorker:
        super().__enter__()
        self._exit_stack.enter_context(self.portal.wrap_async_context_manager(self._worker))
        return self

    def stop(self) -> None:
        self.portal.call(self._worker.stop)
