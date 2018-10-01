:mod:`apscheduler.jobstores.peewee`
=======================================

.. automodule:: apscheduler.jobstores.peewee

API
---

.. autoclass:: PeeWeeJobStore(database=None, database_type=None, database_ref=None, tablename='apscheduler_jobs', pickle_protocol=pickle.HIGHEST_PROTOCOL, tableschema=None, database_options=None)
    :show-inheritance:


Introduction
------------

PeeweeJobStore stores jobs in any relational database management system supported by
`Peewee <http://www.peewee-orm.com/>`_. It can use either a preconfigured
`Database <http://docs.peewee-orm.com/en/latest/peewee/database.html>`_ or you can pass it a
connection URL.

.. list-table::
   :widths: 1 4

   * - External dependencies
     - `Peewee <https://pypi.org/project/peewee/>`_ (+ the backend specific driver package)
   * - Example
     - ``examples/jobstores/peewee_.py``
       (`view online <https://github.com/agronholm/apscheduler/tree/master/examples/jobstores/peewee_.py>`_).
