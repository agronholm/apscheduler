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
     - * Python >= 3.4: none
       * Python 3.3: `asyncio <https://pypi.python.org/pypi/asyncio/>`_
       * Python <= 3.2: `trollius <https://pypi.python.org/pypi/trollius/>`_
   * - Example
     - ``examples/schedulers/asyncio_.py``
       (`view online <https://bitbucket.org/agronholm/apscheduler/src/master/examples/schedulers/asyncio_.py>`_).
