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

* No (hard) external dependencies
* Thread-safe API
* Excellent test coverage (tested on Python 2.4 - 2.7, 3.1 - 3.2, Jython 2.5.2 and PyPy 1.4.1)
* Configurable scheduling mechanisms (triggers):

  * Cron-like scheduling
  * Delayed scheduling of single run jobs (like the UNIX "at" command)
  * Interval-based (run a job at specified time intervals)
* Persistent, stateful jobs
* Multiple, simultaneously active job stores:

  * RAM 
  * File-based simple database (shelve)
  * `SQLAlchemy <http://www.sqlalchemy.org/>`_ (any supported RDBMS works)
  * `MongoDB <http://www.mongodb.org/>`_


Documentation
=============

Documentation can be found `here <http://packages.python.org/APScheduler/>`_.


Source
======

The source can be browsed at `Bitbucket
<http://bitbucket.org/agronholm/apscheduler/src/>`_.
