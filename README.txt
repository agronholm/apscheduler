Advanced Python Scheduler (APScheduler) is a light but powerful in-process task
scheduler that lets you schedule jobs (functions or any python callables) to be
executed at times of your choosing.

The development of APScheduler was heavily influenced by the `Quartz
<http://www.opensymphony.com/quartz/>`_ task scheduler written in Java, and
it provides most of the major features that Quartz does.


Features
========

* No external dependencies
* Thread-safe API
* Configurable scheduling mechanisms (triggers):
  * Cron-like scheduling
  * Delayed scheduling of single fire jobs (like the UNIX "at" command)
  * Interval-based scheduling of jobs, with configurable start date and
    repeat count
* Configurable job stores:
  * RAM
  * File-based simple database (shelve)
  * `SQLAlchemy <http://www.sqlalchemy.org/>`_ (any supported RDBMS works)


Documentation
=============

Documentation can be found `here <http://packages.python.org/APScheduler/>`_.


Source
======

The source can be browsed at `Bitbucket
<http://bitbucket.org/agronholm/apscheduler/src/>`_.
