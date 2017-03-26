:mod:`apscheduler.jobstores.memory`
===================================

.. automodule:: apscheduler.jobstores.memory

API
---

.. autoclass:: MemoryJobStore
    :show-inheritance:


Introduction
------------

MemoryJobStore stores jobs in memory as-is, without serializing them. This allows you to schedule callables that are
unreachable globally and use job non-serializable job arguments.

.. list-table::
   :widths: 1 4

   * - External dependencies
     - none
   * - Example
     - ``examples/schedulers/blocking.py``
       (`view online <https://github.com/agronholm/apscheduler/tree/master/examples/schedulers/blocking.py>`_).

.. caution:: Unlike with other job stores, changes made to any mutable job arguments persist across job invocations.
   You can use this to your advantage, however.
