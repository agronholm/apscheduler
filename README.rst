.. image:: https://github.com/agronholm/apscheduler/actions/workflows/test.yml/badge.svg
  :target: https://github.com/agronholm/apscheduler/actions/workflows/test.yml
  :alt: Build Status
.. image:: https://coveralls.io/repos/github/agronholm/apscheduler/badge.svg?branch=master
  :target: https://coveralls.io/github/agronholm/apscheduler?branch=master
  :alt: Code Coverage
.. image:: https://readthedocs.org/projects/apscheduler/badge/?version=latest
  :target: https://apscheduler.readthedocs.io/en/master/?badge=latest
  :alt: Documentation

.. warning:: The v4.0 series is provided as a **pre-release** and may change in a
   backwards incompatible fashion without any migration pathway, so do NOT use this
   release in production!

Advanced Python Scheduler (APScheduler) is a task scheduler and task queue system for
Python. It can be used solely as a job queuing system if you have no need for task
scheduling. It scales both up and down, and is suitable for both trivial, single-process
use cases as well as large deployments spanning multiple nodes. Multiple schedulers and
workers can be deployed to use a shared data store to provide both a degree of high
availability and horizontal scaling.

APScheduler comes in both synchronous and asynchronous flavors, making it a good fit for
both traditional, thread-based applications, and asynchronous (asyncio or Trio_)
applications. Documentation and examples are provided for integrating with either WSGI_
or ASGI_ compatible web applications.

Support is provided for persistent storage of schedules and jobs. This means that they
can be shared among multiple scheduler/worker instances and will survive process and
node restarts.

The built-in persistent data store back-ends are:

* PostgreSQL
* MySQL and derivatives
* SQLite
* MongoDB

The built-in event brokers (needed in scenarios with multiple schedulers and/or
workers):

* PostgreSQL
* Redis
* MQTT

The built-in scheduling mechanisms (*triggers*) are:

* Cron-style scheduling
* Interval-based scheduling (runs tasks on even intervals)
* Calendar-based scheduling (runs tasks on intervals of X years/months/weeks/days,
  always at the same time of day)
* One-off scheduling (runs a task once, at a specific date/time)

Different scheduling mechanisms can even be combined with so-called *combining triggers*
(see the documentation_ for details).

You can also implement your custom scheduling logic by building your own trigger class.
These will be treated no differently than the built-in ones.

Other notable features include:

* You can limit the maximum number of simultaneous jobs for a given task (function)
* You can limit the amount of time a job is allowed to start late
* Jitter (adjustable, random delays added to the run time of each scheduled job)

.. _Trio: https://pypi.org/project/trio/
.. _WSGI: https://wsgi.readthedocs.io/en/latest/what.html
.. _ASGI: https://asgi.readthedocs.io/en/latest/index.html
.. _documentation: https://apscheduler.readthedocs.io/en/master/

Documentation
=============

Documentation can be found
`here <https://apscheduler.readthedocs.io/en/master/?badge=latest>`_.

Source
======

The source can be browsed at `Github <https://github.com/agronholm/apscheduler>`_.

Reporting bugs
==============

A `bug tracker <https://github.com/agronholm/apscheduler/issues>`_ is provided by
GitHub.

Getting help
============

If you have problems or other questions, you can either:

* Ask in the `apscheduler <https://gitter.im/apscheduler/Lobby>`_ room on Gitter
* Post a question on `GitHub discussions`_, or
* Post a question on StackOverflow_ and add the ``apscheduler`` tag

.. _GitHub discussions: https://github.com/agronholm/apscheduler/discussions/categories/q-a
.. _StackOverflow: http://stackoverflow.com/questions/tagged/apscheduler
