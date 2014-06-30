:mod:`apscheduler.jobstores.sqlalchemy`
=======================================

.. automodule:: apscheduler.jobstores.sqlalchemy

API
---

.. autoclass:: SQLAlchemyJobStore(url=None, engine=None, tablename='apscheduler_jobs', metadata=None, pickle_protocol=pickle.HIGHEST_PROTOCOL)
    :show-inheritance:


Introduction
------------

SQLAlchemyJobStore stores jobs in any relational database management system supported by
`SQLAlchemy <http://www.sqlalchemy.org/>`_. It can use either a preconfigured
`Engine <http://docs.sqlalchemy.org/en/latest/core/connections.html#sqlalchemy.engine.Engine>`_ or you can pass it a
connection URL.

.. list-table::
   :widths: 1 4

   * - External dependencies
     - `SQLAlchemy <https://pypi.python.org/pypi/SQLAlchemy/>`_ (+ the backend specific driver package)
   * - Example
     - ``examples/jobstores/sqlalchemy_.py``
       (`view online <https://bitbucket.org/agronholm/apscheduler/src/master/examples/jobstores/sqlalchemy_.py>`_).
