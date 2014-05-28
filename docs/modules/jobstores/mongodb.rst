:mod:`apscheduler.jobstores.mongodb`
====================================

.. automodule:: apscheduler.jobstores.mongodb

API
---

.. autoclass:: MongoDBJobStore(database='apscheduler', collection='jobs', client=None, pickle_protocol=pickle.HIGHEST_PROTOCOL, **connect_args)
    :show-inheritance:


Introduction
------------

MongoDBJobStore stores jobs in a `MongoDB <http://www.mongodb.com/>`_ database.

.. list-table::
   :widths: 1 4

   * - External dependencies
     - `pymongo <https://pypi.python.org/pypi/pymongo/>`_
   * - Example
     - ``examples/jobstores/mongodb.py``
       (`view online <https://bitbucket.org/agronholm/apscheduler/src/master/examples/jobstores/mongodb.py>`_).
