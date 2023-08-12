:mod:`apscheduler.schedulers.qt`
================================

.. automodule:: apscheduler.schedulers.qt

API
---

.. autoclass:: QtScheduler
    :show-inheritance:


Introduction
------------

QtScheduler lets you integrate APScheduler with your `PySide <https://en.wikipedia.org/wiki/PySide>`_ or
`PyQt <http://www.riverbankcomputing.co.uk/software/pyqt/intro>`_ application.

.. list-table::
   :widths: 1 4

   * - Default executor
     - :class:`~apscheduler.executors.pool.PoolExecutor`
   * - External dependencies
     - PySide or PyQt
   * - Example
     - ``examples/schedulers/qt.py``
       (`view online <https://github.com/agronholm/apscheduler/tree/3.x/examples/schedulers/qt.py>`_).
