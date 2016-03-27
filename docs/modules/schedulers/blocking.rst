:mod:`apscheduler.schedulers.blocking`
======================================

.. automodule:: apscheduler.schedulers.blocking

API
---

.. autoclass:: BlockingScheduler
    :show-inheritance:


Introduction
------------

BlockingScheduler is the simplest possible scheduler. It runs in the foreground, so when you call
:meth:`~apscheduler.schedulers.blocking.BlockingScheduler.start`, the call never returns.

BlockingScheduler can be useful if you want to use APScheduler as a standalone scheduler (e.g. to build a daemon).

.. list-table::
   :widths: 1 4

   * - Default executor
     - :class:`~apscheduler.executors.pool.PoolExecutor`
   * - External dependencies
     - none
   * - Example
     - ``examples/schedulers/blocking.py``
       (`view online <https://github.com/agronholm/apscheduler/tree/master/examples/schedulers/blocking.py>`_).
