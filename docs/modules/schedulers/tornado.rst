:mod:`apscheduler.schedulers.tornado`
=====================================

.. automodule:: apscheduler.schedulers.tornado

API
---

.. autoclass:: TornadoScheduler
    :show-inheritance:


Introduction
------------

TornadoScheduler was meant to be used in `Tornado <http://www.tornadoweb.org/en/stable/>`_ applications.

.. list-table::
   :widths: 1 4

   * - Default executor
     - :class:`~apscheduler.executors.pool.PoolExecutor`
   * - External dependencies
     -  `tornado <https://pypi.python.org/pypi/tornado/>`_
   * - Example
     - ``examples/schedulers/tornado_.py``
       (`view online <https://github.com/agronholm/apscheduler/tree/master/examples/schedulers/tornado_.py>`_)
