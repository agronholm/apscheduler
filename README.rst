Advanced Python Scheduler (APScheduler) is a light but powerful in-process task
scheduler that lets you schedule jobs (functions or any python callables) to be
executed at times of your choosing.

This can be a far better alternative to externally run cron scripts for
long-running applications (e.g. web applications), as it is platform neutral
and can directly access your application's variables and functions.

The development of APScheduler was heavily influenced by the `Quartz
<http://www.quartz-scheduler.org/>`_ task scheduler written in Java.
APScheduler provides most of the major features that Quartz does, but it also
provides features not present in Quartz (such as multiple job stores).


Features
========

* Thread-safe API
* Excellent test coverage (tested on CPython 2.6 - 2.7, 3.2 - 3.4, PyPy 2.3)
* Configurable scheduling mechanisms (triggers):

  * Cron-like scheduling
  * Delayed scheduling of single run jobs (like the UNIX "at" command)
  * Interval-based (run a job at specified time intervals)
* Integrates with several frameworks:

  * `asyncio <http://docs.python.org/3.4/library/asyncio.html>`_
    (`PEP 3156 <http://www.python.org/dev/peps/pep-3156/>`_)
  * `gevent <http://www.gevent.org/>`_
  * `Tornado <http://www.tornadoweb.org/>`_
  * `Twisted <http://twistedmatrix.com/>`_
  * `Qt <http://qt-project.org/>`_ (using either `PyQt <http://www.riverbankcomputing.com/software/pyqt/intro>`_
    or `PySide <http://qt-project.org/wiki/PySide>`_)
* Multiple, simultaneously active job stores:

  * Memory
  * `SQLAlchemy <http://www.sqlalchemy.org/>`_ (any supported RDBMS works)
  * `MongoDB <http://www.mongodb.org/>`_
  * `Redis <http://redis.io/>`_


Documentation
=============

Documentation can be found `here <http://readthedocs.org/docs/apscheduler/en/latest/>`_.


Source
======

The source can be browsed at `Bitbucket <http://bitbucket.org/agronholm/apscheduler/src/>`_.


Reporting bugs
==============

A `bug tracker <https://bitbucket.org/agronholm/apscheduler/issues?status=new&status=open>`_
is provided by bitbucket.org.


Getting help
============

If you have problems or other questions, you can either:

* Ask on the `APScheduler Google group <http://groups.google.com/group/apscheduler>`_, or
* Ask on the ``#apscheduler`` channel on `Freenode IRC <http://freenode.net/irc_servers.shtml>`_
