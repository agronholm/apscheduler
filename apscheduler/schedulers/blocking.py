from __future__ import absolute_import
from threading import Event

from apscheduler.schedulers.base import BaseScheduler


class BlockingScheduler(BaseScheduler):
    """
    A scheduler that runs in the foreground (:meth:`~apscheduler.schedulers.base.BaseScheduler.start` will block).
    """

    MAX_WAIT_TIME = 4294967  # Maximum value accepted by Event.wait() on Windows

    _event = None

    def start(self):
        super(BlockingScheduler, self).start()
        self._event = Event()
        self._main_loop()

    def shutdown(self, wait=True):
        super(BlockingScheduler, self).shutdown(wait)
        self._event.set()

    def _main_loop(self):
        while self.running:
            wait_seconds = self._process_jobs()
            self._event.wait(wait_seconds if wait_seconds is not None else self.MAX_WAIT_TIME)
            self._event.clear()

    def wakeup(self):
        self._event.set()
