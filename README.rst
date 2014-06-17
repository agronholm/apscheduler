Advanced Python Scheduler (APScheduler) is a Python library that lets you schedule your Python code to be executed
later, either just once or periodically. You can add new jobs or remove old ones on the fly as you please. If you store
your jobs in a database, they will also survive scheduler restarts and maintain their state. When the scheduler is
restarted, it will then run all the jobs it should have run while it was offline [#f1]_.

Among other things, APScheduler can be used as a cross-platform, application specific replacement to platform specific
schedulers, such as the cron daemon or the Windows task scheduler. Please note, however, that APScheduler is **not** a
daemon or service itself, nor does it come with any command line tools. It is primarily meant to be run inside existing
applications. That said, APScheduler does provide some building blocks for you to build a scheduler service or to run a
dedicated scheduler process.

APScheduler has three built-in scheduling systems you can use:

* Cron-style scheduling (with optional start/end times)
* Interval-based execution (runs jobs on even intervals, with optional start/end times)
* One-off delayed execution (runs jobs once, on a set date/time)

You can mix and match scheduling systems and the backends where the jobs are stored any way you like.
Supported backends for storing jobs include:

* Memory
* `SQLAlchemy <http://www.sqlalchemy.org/>`_ (any RDBMS supported by SQLAlchemy works)
* `MongoDB <http://www.mongodb.org/>`_
* `Redis <http://redis.io/>`_

APScheduler also integrates with several common Python frameworks, like:

* `asyncio <http://docs.python.org/3.4/library/asyncio.html>`_ (:pep:`3156`)
* `gevent <http://www.gevent.org/>`_
* `Tornado <http://www.tornadoweb.org/>`_
* `Twisted <http://twistedmatrix.com/>`_
* `Qt <http://qt-project.org/>`_ (using either `PyQt <http://www.riverbankcomputing.com/software/pyqt/intro>`_
  or `PySide <http://qt-project.org/wiki/PySide>`_)

.. [#f1] The cutoff period for this is also configurable.


Documentation
-------------

Documentation can be found `here <http://readthedocs.org/docs/apscheduler/en/latest/>`_.


Source
------

The source can be browsed at `Bitbucket <http://bitbucket.org/agronholm/apscheduler/src/>`_.


Reporting bugs
--------------

A `bug tracker <https://bitbucket.org/agronholm/apscheduler/issues?status-new&status-open>`_
is provided by bitbucket.org.


Getting help
------------

If you have problems or other questions, you can either:

* Ask on the ``#apscheduler`` channel on `Freenode IRC <http://freenode.net/irc_servers.shtml>`_
* Ask on the `APScheduler Google group <http://groups.google.com/group/apscheduler>`_, or
* Ask on `StackOverflow <http://stackoverflow.com/questions/tagged/apscheduler>`_ and tag your question with the
  ``apscheduler`` tag
