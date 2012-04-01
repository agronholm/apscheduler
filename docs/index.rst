Advanced Python Scheduler
=========================

.. contents::

Introduction
------------

Advanced Python Scheduler (APScheduler) is a light but powerful in-process task
scheduler that lets you schedule functions (or any other python callables) to be
executed at times of your choosing.

This can be a far better alternative to externally run cron scripts for
long-running applications (e.g. web applications), as it is platform neutral
and can directly access your application's variables and functions.

The development of APScheduler was heavily influenced by the `Quartz
<http://www.quartz-scheduler.org/>`_ task scheduler written in Java.
APScheduler provides most of the major features that Quartz does, but it also
provides features not present in Quartz (such as multiple job stores).


Features
--------

* No (hard) external dependencies
* Thread-safe API
* Excellent test coverage (tested on CPython 2.4 - 2.7, 3.1 - 3.2, Jython 2.5.2, PyPy 1.4.1 and 1.5)
* Configurable scheduling mechanisms (triggers):

  * Cron-like scheduling
  * Delayed scheduling of single run jobs (like the UNIX "at" command)
  * Interval-based (run a job at specified time intervals)
* Multiple, simultaneously active job stores:

  * RAM 
  * File-based simple database (shelve)
  * `SQLAlchemy <http://www.sqlalchemy.org/>`_ (any supported RDBMS works)
  * `MongoDB <http://www.mongodb.org/>`_


Usage
=====

Installing APScheduler
----------------------

On Python 3.3 or later (or if you have
`distutils2 <http://pypi.python.org/pypi/Distutils2>` installed) you can do::

    $ pysetup install apscheduler

Otherwise, you can use `pip <http://pypi.python.org/pypi/pip/>`_::

    $ pip install apscheduler

or `easy_install <http://pypi.python.org/pypi/distribute/>`_::

	$ easy_install apscheduler

If that doesn't work, you can manually `download the APScheduler distribution
<http://pypi.python.org/pypi/APScheduler/>`_ from PyPI, extract and then
install it::

    $ python setup.py install


Starting the scheduler
----------------------

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
to use a custom trigger, you need to use the
:meth:`~apscheduler.scheduler.Scheduler.add_job` method.

When a scheduled job is triggered, it is handed over to the thread pool for
execution.

You can request a job to be added to a specific job store by giving the target
job store's alias in the ``jobstore`` option to
:meth:`~apscheduler.scheduler.Scheduler.add_job` or any of the above shortcut
methods.

You can schedule jobs on the scheduler **at any time**. If the scheduler is not
running when the job is added, the job will be scheduled `tentatively` and its
first run time will only be computed when the scheduler starts. Jobs will not
run retroactively in such cases.


Shutting down the scheduler
---------------------------

To shut down the scheduler::

    sched.shutdown()

By default, the scheduler shuts down its thread pool and waits until all
currently executing jobs are finished. For a faster exit you can do::

    sched.shutdown(wait=False)

This will still shut down the thread pool but does not wait for any running
tasks to complete. Also, if you gave the scheduler a thread pool that you want
to manage elsewhere, you probably want to skip the thread pool shutdown
altogether::

    sched.shutdown(shutdown_threadpool=False)

This implies ``wait=False``, since there is no way to wait for the scheduler's
tasks to finish without shutting down the thread pool.


Scheduler configuration options
-------------------------------

======================= ========== ==============================================
Directive               Default    Definition
======================= ========== ==============================================
misfire_grace_time      1          Maximum time in seconds for the job execution
                                   to be allowed to delay before it is considered
                                   a misfire
coalesce                False      Roll several pending executions of jobs into one
daemonic                True       Controls whether the scheduler thread is
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
threadpool              (built-in) Instance of a :pep:`3148` compliant thread
                                   pool or a dot-notation (``x.y.z:varname``)
                                   reference to one
threadpool.core_threads 0          Maximum number of persistent threads in the pool
threadpool.max_threads  20         Maximum number of total threads in the pool
threadpool.keepalive    1          Seconds to keep non-core worker threads waiting
                                   for new tasks
jobstores.X.class                  Class of the jobstore named X (specified as
                                   module.name:classname)
jobstores.X.Y                      Constructor option Y of jobstore X
======================= ========== ==============================================


Job stores
----------

APScheduler keeps all the scheduled jobs in *job stores*. Job stores are
configurable adapters to some back-end that may or may not support persisting
job configurations on disk, database or something else. Job stores are added
to the scheduler and identified by their aliases. The alias ``default`` is
special in that if the user does not explicitly specify a job store alias when
scheduling a job, it goes to the ``default`` job store. If there is no job
store in the scheduler by that name when the scheduler is started, a new job
store of type :class:`~apscheduler.jobstores.ram_store.RAMJobStore` is created
to serve as the default.

The other built-in job stores are:

* :class:`~apscheduler.jobstores.shelve_store.ShelveJobStore`
* :class:`~apscheduler.jobstores.sqlalchemy_store.SQLAlchemyJobStore`
* :class:`~apscheduler.jobstores.mongodb_store.MongoDBJobStore`

Job stores can be added either through configuration options or the
:meth:`~apscheduler.scheduler.Scheduler.add_jobstore` method. The following
are therefore equal::

    config = {'apscheduler.jobstores.file.class': 'apscheduler.jobstores.shelve_store:ShelveJobStore',
              'apscheduler.jobstores.file.path': '/tmp/dbfile'}
    sched = Scheduler(config)

and::

    from apscheduler.jobstores.shelve_store import ShelveJobStore

	sched = Scheduler()
	sched.add_jobstore(ShelveJobStore('/tmp/dbfile'), 'file')

The example configuration above results in the scheduler having two
job stores -- one
:class:`~apscheduler.jobstores.ram_store.RAMJobStore` and one
:class:`~apscheduler.jobstores.shelve_store.ShelveJobStore`.


Job persistency
---------------

The built-in job stores (other than
:class:`~apscheduler.jobstores.ram_store.RAMJobStore`) store jobs in a durable
manner. This means that when you schedule jobs in them, shut down the scheduler,
restart it and readd the job store in question, it will load the previously
scheduled jobs automatically.

Persistent job stores store a reference to the target callable in text form
and serialize the arguments using pickle. This unfortunately adds some
restrictions:

* You cannot schedule static methods, inner functions or lambdas.
* You cannot update the objects given as arguments to the callable.

Technically you *can* update the state of the argument objects, but those
changes are never persisted back to the job store.

.. note:: None of these restrictions apply to ``RAMJobStore``.


Limiting the number of concurrently executing instances of a job
----------------------------------------------------------------

By default, no two instances of the same job will be run concurrently. This
means that if the job is about to be run but the previous run hasn't finished
yet, then the latest run is considered a misfire. It is possible to set the
maximum number of instances for a particular job that the scheduler will let
run concurrently, by using the ``max_instances`` keyword argument when adding
the job.


Coalescing job executions
-------------------------

Sometimes the scheduler may be unable to execute a scheduled job at the time
it was scheduled to run. The most common case is when a job is scheduled in a
persistent job store and the scheduler is shut down and restarted after the job
was supposed to execute. Normally the scheduler would execute the job as many
times as its misfire_grace_time option permits. This can be undesireable in
many cases, such as backing up data or sending notifications. By setting the
``coalesce`` option to ``True`` when adding the job (or globally on the
scheduler) you can avoid unintended successive executions of the job. The
bypassed runs of the job are not considered misfires nor do they count towards
any maximum run count of the job.


Scheduler events
----------------

It is possible to attach event listeners to the scheduler. Scheduler events are
fired on certain occasions, and may carry additional information in them
concerning the details of that particular event. It is possible to listen to
only particular types of events by giving the appropriate ``mask`` argument to
:meth:`~apscheduler.scheduler.Scheduler.add_listener`, OR'ing
the different constants together. The listener callable is called with one
argument, the event object. The type of the event object is tied to the event
code as shown below:

========================== ============== ==========================================
Constant                   Event class    Triggered when...
========================== ============== ==========================================
EVENT_SCHEDULER_START      SchedulerEvent The scheduler is started
EVENT_SCHEDULER_SHUTDOWN   SchedulerEvent The scheduler is shut down
EVENT_JOBSTORE_ADDED       JobStoreEvent  A job store is added to the scheduler
EVENT_JOBSTORE_REMOVED     JobStoreEvent  A job store is removed from the scheduler
EVENT_JOBSTORE_JOB_ADDED   JobStoreEvent  A job is added to a job store
EVENT_JOBSTORE_JOB_REMOVED JobStoreEvent  A job is removed from a job store
EVENT_JOB_EXECUTED         JobEvent       A job is executed successfully
EVENT_JOB_ERROR            JobEvent       A job raised an exception during execution
EVENT_JOB_MISSED           JobEvent       A job's execution time is missed
========================== ============== ==========================================

See the documentation for the :mod:`~apscheduler.events` module for specifics
on the available event attributes.

Example::

    def my_listener(event):
        if event.exception:
            print 'The job crashed :('
        else:
            print 'The job worked :)'

    scheduler.add_listener(my_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)


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


Extending APScheduler
=====================

It is possible to extend APScheduler to support alternative job stores and
triggers. See the :doc:`Extending APScheduler <extending>` document for details.


FAQ
===

Q: Why do my processes hang instead of exiting when they are finished?

A: A scheduled job may still be executing. APScheduler's thread pool is wired
to wait for the job threads to exit before allowing the interpreter to exit to
avoid unpredictable behavior caused by the shutdown procedures of the Python
interpreter. A more thorough explanation
`can be found here <http://joeshaw.org/2009/02/24/605>`_.


Reporting bugs
==============

A `bug tracker <http://bitbucket.org/agronholm/apscheduler/issues/>`_
is provided by bitbucket.org.


Getting help
============

If you have problems or other questions, you can either:

* Ask on the `APScheduler Google group
  <http://groups.google.com/group/apscheduler>`_, or
* Ask on the ``#apscheduler`` channel on
  `Freenode IRC <http://freenode.net/irc_servers.shtml>`_


.. include:: ../CHANGES.rst

