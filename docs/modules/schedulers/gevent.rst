:mod:`apscheduler.schedulers.gevent`
====================================

.. automodule:: apscheduler.schedulers.gevent

API
---

.. autoclass:: GeventScheduler
    :show-inheritance:


Introduction
------------

GeventScheduler was meant to be used with applications that use `gevent <http://www.gevent.org/>`_.
GeventScheduler uses gevent natively, so it doesn't require monkey patching. By default it executes jobs as greenlets.

.. list-table::
   :widths: 1 4

   * - Default executor
     - :class:`~apscheduler.executors.gevent.GeventExecutor`
   * - External dependencies
     - `gevent <https://pypi.python.org/pypi/gevent/>`_
   * - Example
     - ``examples/schedulers/gevent_.py``
       (`view online <https://github.com/agronholm/apscheduler/tree/3.x/examples/schedulers/gevent_.py>`_).
