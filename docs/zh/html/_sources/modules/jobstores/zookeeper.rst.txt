:mod:`apscheduler.jobstores.zookeeper`
======================================

.. automodule:: apscheduler.jobstores.zookeeper

API
---

.. autoclass:: ZooKeeperJobStore(path='/apscheduler', client=None, close_connection_on_exit=False, pickle_protocol=pickle.HIGHEST_PROTOCOL, **connect_args)
    :show-inheritance:


Introduction
------------

ZooKeeperJobStore stores jobs in an `Apache ZooKeeper <https://zookeeper.apache.org/>`_ instance.

.. list-table::
   :widths: 1 4

   * - External dependencies
     - `kazoo <https://pypi.python.org/pypi/kazoo>`_
   * - Example
     - ``examples/jobstores/zookeeper.py``
       (`view online <https://github.com/agronholm/apscheduler/tree/master/examples/jobstores/zookeeper.py>`_).
