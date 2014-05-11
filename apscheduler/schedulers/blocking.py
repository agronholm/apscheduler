from __future__ import absolute_import
from threading import Event

import six

from apscheduler.schedulers.base import BaseScheduler


class BlockingScheduler(BaseScheduler):
    """A scheduler that runs in the foreground. Calling :meth:`start` will block."""

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
            self._event.wait(wait_seconds or six.MAXSIZE)
            self._event.clear()

    def _wakeup(self):
        self._event.set()
