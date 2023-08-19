:mod:`apscheduler.schedulers.asyncio`
=====================================

.. automodule:: apscheduler.schedulers.asyncio

API
---

.. autoclass:: AsyncIOScheduler
    :show-inheritance:


Introduction
------------

AsyncIOScheduler was meant to be used with the `AsyncIO <https://docs.python.org/3/library/asyncio.html>`_ event loop.
By default, it will run jobs in the event loop's thread pool.

If you have an application that runs on an AsyncIO event loop, you will want to use this scheduler.

.. list-table::
   :widths: 1 4

   * - Default executor
     - :class:`~apscheduler.executors.asyncio.AsyncIOExecutor`
   * - External dependencies
     - none
   * - Example
     - ``examples/schedulers/asyncio_.py``
       (`view online <https://github.com/agronholm/apscheduler/tree/3.x/examples/schedulers/asyncio_.py>`_).
