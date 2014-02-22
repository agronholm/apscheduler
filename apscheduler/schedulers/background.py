from __future__ import absolute_import
from threading import Thread, Event

from apscheduler.schedulers.base import BaseScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.util import asbool


class BackgroundScheduler(BlockingScheduler):
    """A scheduler that runs in the background using a separate thread."""

    _thread = None

    def _configure(self, config):
        self._daemon = asbool(config.pop('daemon', True))
        super(BackgroundScheduler, self)._configure(config)

    def start(self):
        BaseScheduler.start(self)
        self._event = Event()
        self._thread = Thread(target=self._main_loop, name='APScheduler')
        self._thread.daemon = self._daemon
        self._thread.start()

    def shutdown(self, wait=True):
        super(BackgroundScheduler, self).shutdown(wait)
        self._thread.join()
        del self._thread
