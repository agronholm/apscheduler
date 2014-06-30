:mod:`apscheduler.schedulers.twisted`
=====================================

.. automodule:: apscheduler.schedulers.twisted

API
---

.. autoclass:: TwistedScheduler
    :show-inheritance:


Introduction
------------

TwistedScheduler was meant to be used in `Twisted <https://twistedmatrix.com/trac/>`_ applications.
By default it uses the reactor's thread pool to execute jobs.

.. list-table::
   :widths: 1 4

   * - Default executor
     - :class:`~apscheduler.executors.twisted.TwistedExecutor`
   * - External dependencies
     - `twisted <https://pypi.python.org/pypi/Twisted/>`_
   * - Example
     - ``examples/schedulers/twisted_.py``
       (`view online <https://bitbucket.org/agronholm/apscheduler/src/master/examples/schedulers/twisted_.py>`_).
