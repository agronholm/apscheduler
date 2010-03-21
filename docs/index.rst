Advanced Python Scheduler
=========================

APScheduler is a light but powerful in-process task scheduler that
lets you schedule functions (or any python callables) to be executed at times
of your choosing.

This can be a far better alternative to externally run cron scripts for
long-running applications (e.g. web applications), as it is platform neutral
and can directly access your application's variables and functions.

**Features:**

* No external dependencies
* Thread-safe API
* Cron-like scheduling
* Delayed scheduling of single fire jobs (like the UNIX "at" command)
* Interval-based scheduling of jobs, with configurable start date and
  repeat count


Installation
============

To install, you can either:

* `Download the APScheduler package
  <http://pypi.python.org/pypi/APScheduler/>`_ from PyPI, extract it and then
  install it::

    $ python setup.py install

* Or, if you have either
  `distribute <http://pypi.python.org/pypi/distribute>`_ 
  or setuptools installed, you can use easy_install::

	$ easy_install apscheduler


Usage
=====

Starting up
-----------

::

    from apscheduler.scheduler import Scheduler
    
    sched = Scheduler()
    sched.start()

Notice that you can schedule jobs on the scheduler **at any time**.
Of course, no jobs will be executed as long as the scheduler isn't running.
Also, jobs scheduled before the scheduler was started will not fire
retroactively.

Configuration
-------------

All option keys can be prefixed with ``apscheduler.`` to avoid name clashes
in an .ini file.

You can also configure the scheduler after its instantiation, if necessary.
This is handy if you use the decorators for scheduling and must have a
Scheduler instance available from the very beginning::

    from apscheduler.scheduler import Scheduler
    
    sched = Scheduler()
    
    @sched.interval_schedule(hours=3)
    def some_job():
        print "Decorated job"
    
    sched.configure(options_from_ini_file)
    sched.start()

Available configuration options:

======================= ======== ==============================================
Directive               Default  Definition
======================= ======== ==============================================
misfire_grace_time      1        If the scheduler misses the execution of a task 
                                 (due to high CPU load for example) by an amount 
                                 of seconds equal to or less than this value, 
                                 then it will still be executed.
daemonic                True     Controls whether the scheduler thread is
                                 daemonic or not.
                                 
                                 If set to ``False``, then the
                                 scheduler must be shut down explicitly
                                 when the program is about to finish, or it will
                                 prevent the program from terminating.

                                 If set to ``True``, the scheduler will
                                 automatically terminate with the application,
                                 but may cause an exception to be raised on
                                 exit.
                                 
                                 Jobs are always executed in non-daemonic
                                 threads.
======================= ======== ==============================================

Scheduling jobs
---------------

There are three methods for scheduling new jobs:

.. toctree::
   :maxdepth: 1

   dateschedule
   intervalschedule
   cronschedule

When a scheduled job is triggered, a new thread will be created and the
callable is executed in that thread. A new thread is always created for
each execution of the job.

One thing you should keep in mind is that no two instances of the same job
will ever be run concurrently. This means that if the scheduler is attempting
to execute a job, but the previously launched thread for that job is still
running, then it will not be fired at the time. Only when the previous instance
has finished, will it be executed again.

Shutting down
-------------

::

    sched.shutdown()

A scheduler that has been shut down can be restarted, but the shutdown
procedure clears the job list, so you will have to reschedule any jobs
you want executed.

If you want to make sure that the scheduler has really terminated, you
can specify a timeout (in seconds)::

    sched.shutdown(10)

This will wait at most 10 seconds for the scheduler thread to terminate,
and then proceed anyways.

To make sure that the scheduler has terminated, you can specify
a timeout of 0. This will disable the waiting timeout and will wait as long as
it takes for the scheduler to shut down.

.. note::
	Shutting down the scheduler does not guarantee that all jobs have
	terminated.


FAQ
===

Q: Why do my processes hang instead of exiting when they are finished?

A: A scheduled job may still be executing. Job threads are *non-daemonic*, and a
program will not exit as long as any non-daemonic threads are running. The
reason why job threads are non-daemonic is that the shutdown procedures of
the Python interpreter may cause unspecified behavior in the executing job.
A more thorough explanation
`can be found here <http://joeshaw.org/2009/02/24/605>`_.


Getting help
============

If you have problems or other questions, you can either:

* Join the ``#apscheduler`` channel on
  `Freenode IRC <http://freenode.net/irc_servers.shtml>`_, or
* Send email to <apscheduler at nextday dot fi>


Reporting bugs
==============

A `bug tracker <http://bitbucket.org/agronholm/apscheduler/issues/>`_
is provided by bitbucket.org.


.. include:: ../CHANGES.txt


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

