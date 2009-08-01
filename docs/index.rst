Advanced Python Scheduler
=========================

Advanced Python Scheduler (APScheduler) is an in-process task scheduler that
lets you schedule functions (or any python callables) to be executed at times
of your choosing.

**Features:**

* No external dependencies
* Thread-safe API
* Cron-like scheduling
* Delayed scheduling of single fire jobs (like the UNIX "at" command)
* Interval-based scheduling of jobs, with configurable start date and
  repeat count

Configuration
=============

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
``misfire_grace_time``  1        If the scheduler misses the execution of a task 
                                 (due to high CPU load for example) by an amount 
                                 of seconds equal to or less than this value, 
                                 then it will still be executed.
======================= ======== ==============================================

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

To make sure that the scheduler has been terminated, you can specify
a timeout of 0. This will disable the waiting timeout.

Scheduling jobs
---------------

There are three methods for scheduling new jobs:

.. toctree::
   :maxdepth: 1

   dateschedule
   intervalschedule
   cronschedule

One thing you should keep in mind is that no two instances of the same job
will ever be run concurrently. This means that if the scheduler is attempting
to execute a job, but the previously launched thread for that job is still
running, then it will not be fired at the time. Only when the previous instance
has finished, will it be executed again.


FAQ
===

Q: Why do my processes hang instead of exiting when they are finished?

A: A scheduled job may still be executing. Job threads are *non-daemonic*, and a
program will not exit as long as any non-daemonic threads are running. The
reason why job threads are non-daemonic is that the shutdown procedures of
the Python interpreter may cause unspecified behavior in the executing job.
A more thorough explanation
`can be found here <http://joeshaw.org/2009/02/24/605>`_.


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

