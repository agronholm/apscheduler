Advanced Python Scheduler
=========================

Advanced Python Scheduler (APScheduler) is a light but powerful in-process task
scheduler that lets you schedule functions (or any other python callables) to be
executed at times of your choosing.

This can be a far better alternative to externally run cron scripts for
long-running applications (e.g. web applications), as it is platform neutral
and can directly access your application's variables and functions.

**Features:**

* No (hard) external dependencies
* Thread-safe API
* Excellent test coverage
* Configurable scheduling mechanisms (triggers):

  * Cron-like scheduling
  * Delayed scheduling of single run jobs (like the UNIX "at" command)
  * Interval-based (run a job at specified time intervals)
* Persistent, stateful jobs
* Multiple, simultaneously active job stores:

  * RAM 
  * File-based simple database (shelve)
  * `SQLAlchemy <http://www.sqlalchemy.org/>`_ (any supported RDBMS works)


Installation
============

The preferred method of installation is with
`pip <http://pypi.python.org/pypi/pip/>`_ or
`easy_install <http://pypi.python.org/pypi/distribute/>`_::

    $ pip install apscheduler

or::

	$ easy_install apscheduler

If that doesn't work, you can manually `download the APScheduler package
<http://pypi.python.org/pypi/APScheduler/>`_ from PyPI, extract and then
install it::

    $ python setup.py install


Usage
=====

Startup and configuration
-------------------------

To start the scheduler with default settings::

    from apscheduler.scheduler import Scheduler
    
    sched = Scheduler()
    sched.start()

The constructor takes as its first, optional parameter a dictionary of "global"
options to facilitate configuration from .ini files. All APScheduler options
given in the global configuration must begin with "apscheduler." to avoid name
clashes with other software. The constructor also takes options as keyword
arguments (without the prefix).

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
threadpool                       
threadpool.core_threads 0        Maximum number of persistent threads in the pool
threadpool.max_threads  None     Maximum number of total threads in the pool
threadpool.keepalive    1        Seconds to keep non-core worker threads waiting
                                 for new tasks
jobstore.X.class                 Class of the jobstore named X (specified as
                                 module.name:classname)
jobstore.X.Y                     Constructor option Y of jobstore X
======================= ======== ==============================================


Job stores
----------

APScheduler keeps all the scheduled jobs in `job stores`. Job stores are
configurable adapters to some back-end that may or may not support persisting
job configurations on disk database or something else. Each job store in the
scheduler has an alias. By default, APScheduler only has one job store
configured ("default", of type
:class:`~apscheduler.jobstore.ram_store.RAMJobStore`).
More job stores can be added either through configuration options or the
:meth:`~apscheduler.scheduler.Scheduler.add_jobstore` method. The following
are therefore equal::

    config = {'apscheduler.jobstore.file.class': 'apscheduler.jobstore.shelve_store:ShelveJobStore',
              'apscheduler.jobstore.file.path': '/tmp/dbfile'}
    sched = Scheduler(config)

and::

    from apscheduler.jobstore.shelve_store import ShelveJobStore

	sched = Scheduler()
	sched.add_jobstore(ShelveJobStore('/tmp/dbfile'), 'file')

APScheduler can work with multiple configured job stores. The example
configuration above results in the scheduler having two jobstores -- one
(:class:`~apscheduler.jobstore.ram_store.RAMJobStore`) and one
(:class:`~apscheduler.jobstore.shelve_store.ShelveJobStore`).

In addition to the built-in job stores, it is possible to extend APScheduler to
support other persistence mechanisms as well. See the
:doc:`Extending APScheduler <extending>` section for details.


Persistent job stores
---------------------

Job stores such as (:class:`~apscheduler.jobstore.shelve_store.ShelveJobStore`)
and (:class:`~apscheduler.jobstore.sqlalchemy_store.SQLAlchemyJobStore`) store
jobs in a durable manner. This means that when you schedule jobs in them, shut
down the scheduler, restart it and readd the job store in question, it will
load the previously scheduled jobs automatically. It can automatically find the
callback function using a textual reference saved when the job was added.
Unfortunately this means that **you can only schedule top level functions with
persistent job stores**. The job store will raise an exception if you attempt
to schedule a noncompliant callable.


Triggers
--------

Triggers determine the times when the jobs should be run.
APScheduler comes with three built-in triggers --
:class:`~apscheduler.triggers.simple.SimpleTrigger`,
:class:`~apscheduler.triggers.interval.IntervalTrigger` and
:class:`~apscheduler.triggers.cron.CronTrigger`. You don't normally use these
directly, however. The scheduler has shortcut methods for these built-in
triggers, as discussed in the next section.


Scheduling jobs
---------------

The simplest way to schedule jobs using the built-in triggers is to use one of
the shortcut methods provided by the scheduler:

.. toctree::
   :maxdepth: 1

   dateschedule
   intervalschedule
   cronschedule

These shortcuts cover the vast majority of use cases. However, if you need
to use a custom trigger or schedule a stateful job, you need to use the
:meth:`~apscheduler.scheduler.Scheduler.add_job` method.

When a scheduled job is triggered, it is handed over to the thread pool for
execution.

You can request a job to be added to a specific job store by giving the target
job store's alias in the ``jobstore`` option to
:meth:`~apscheduler.scheduler.Scheduler.add_job` or any of the shortcut
methods.

You can schedule jobs on the scheduler **at any time**. If the scheduler is not
running when the job is added, the job will be scheduled `tentatively` and its
first run time will only be computed when the scheduler starts. Jobs will not
run retroactively in such cases.

No two instances of the same job will ever be run concurrently. This means that
if the job is about to be run but the previous run hasn't finished yet, then
the latest run is counted as a misfire.


Getting a list of scheduled jobs
--------------------------------

If you want to see which jobs are currently added in the scheduler, you can
simply do::

	sched.print_jobs()

This will print a human-readable listing of scheduled jobs, their triggering
mechanisms and the next time they will fire. If you supply a file-like object
as an argument to this method, it will output the results in that file.

To get a machine processable list of the scheduled jobs, you can use the
:meth:`~apscheduler.scheduler.Scheduler.get_jobs` scheduler method. It will
return a list of :class:`~apscheduler.job.Job` instances.


Shutting down
-------------

::

    sched.shutdown()

A scheduler that has been shut down can be restarted, but the shutdown
procedure clears any scheduled transient jobs.

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

A: A scheduled job may still be executing. APScheduler's thread pool is wired
to wait for the job threads to exit before allowing the interpreter to exit to
avoid unpredictable behavior caused by the shutdown procedures of the Python
interpreter. A more thorough explanation
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

