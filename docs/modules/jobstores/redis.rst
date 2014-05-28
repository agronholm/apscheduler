:mod:`apscheduler.jobstores.redis`
========================================

.. automodule:: apscheduler.jobstores.redis

API
---

.. autoclass:: RedisJobStore(db=0, jobs_key='apscheduler.jobs', run_times_key='apscheduler.run_times', pickle_protocol=pickle.HIGHEST_PROTOCOL, **connect_args)
    :show-inheritance:


Introduction
------------

RedisJobStore stores jobs in a `redis <http://redis.io/>`_ database.

.. list-table::
   :widths: 1 4

   * - External dependencies
     - `redis <https://pypi.python.org/pypi/redis/>`_
   * - Example
     - ``examples/jobstores/redis_.py``
       (`view online <https://bitbucket.org/agronholm/apscheduler/src/master/examples/jobstores/redis_.py>`_).
