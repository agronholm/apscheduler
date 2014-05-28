:mod:`apscheduler.schedulers.background`
========================================

.. automodule:: apscheduler.schedulers.background

API
---

.. autoclass:: BackgroundScheduler
    :show-inheritance:


Introduction
------------

BackgroundScheduler runs in a thread **inside** your existing application. Calling
:meth:`~apscheduler.schedulers.blocking.BackgroundScheduler.start` will start the scheduler and it will continue running
after the call returns.

.. list-table::
   :widths: 1 4

   * - Default executor
     - :class:`~apscheduler.executors.pool.PoolExecutor`
   * - External dependencies
     - none
   * - Example
     - ``examples/schedulers/background.py``
       (`view online <https://bitbucket.org/agronholm/apscheduler/src/master/examples/schedulers/background.py>`_).
