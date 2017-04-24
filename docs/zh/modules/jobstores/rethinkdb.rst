:mod:`apscheduler.jobstores.rethinkdb`
======================================

.. automodule:: apscheduler.jobstores.rethinkdb

API
---

.. autoclass:: RethinkDBJobStore(database='apscheduler', table='jobs', client=None, pickle_protocol=pickle.HIGHEST_PROTOCOL, **connect_args)
    :show-inheritance:


Introduction
------------

RethinkDBJobStore stores jobs in a `RethinkDB <https://www.rethinkdb.com/>`_ database.

.. list-table::
   :widths: 1 4

   * - External dependencies
     - `rethinkdb <https://pypi.python.org/pypi/rethinkdb>`_
   * - Example
     - ``examples/jobstores/rethinkdb_.py``
       (`view online <https://github.com/agronholm/apscheduler/tree/master/examples/jobstores/rethinkdb_.py>`_).
