Version history
===============

To find out how to migrate your application from a previous version of
APScheduler, see the :doc:`migration section <migration>`.

3.8.1
-----

* Allowed the use of tzlocal v4.0+ in addition to v2.*


3.8.0
-----

* Allowed passing through keyword arguments to the underlying stdlib executors in the
  thread/process pool executors (PR by Albert Xu)


3.7.0
-----

* Dropped support for Python 3.4
* Added PySide2 support (PR by Abdulla Ibrahim)
* Pinned ``tzlocal`` to a version compatible with pytz
* Ensured that jitter is always non-negative to prevent triggers from firing more often than
  intended
* Changed ``AsyncIOScheduler`` to obtain the event loop in ``start()`` instead of ``__init__()``,
  to prevent situations where the scheduler won't run because it's using a different event loop
  than then one currently running
* Made it possible to create weak references to ``Job`` instances
* Made the schedulers explicitly raise a descriptive ``TypeError`` when serialization is attempted
* Fixed Zookeeper job store using backslashes instead of forward slashes for paths
  on Windows (PR by Laurel-rao)
* Fixed deprecation warnings on the MongoDB job store and increased the minimum PyMongo
  version to 3.0
* Fixed ``BlockingScheduler`` and ``BackgroundScheduler`` shutdown hanging after the user has
  erroneously tried to start it twice
* Fixed memory leak when coroutine jobs raise exceptions (due to reference cycles in tracebacks)
* Fixed inability to schedule wrapped functions with extra arguments when the wrapped function
  cannot accept them but the wrapper can (original PR by Egor Malykh)
* Fixed potential ``where`` clause error in the SQLAlchemy job store when a subclass uses more than
  one search condition
* Fixed a problem where bound methods added as jobs via textual references were called with an
  unwanted extra ``self`` argument (PR by Pengjie Song)
* Fixed ``BrokenPoolError`` in ``ProcessPoolExecutor`` so that it will automatically replace the
  broken pool with a fresh instance


3.6.3
-----

* Fixed Python 2.7 accidentally depending on the ``trollius`` package (regression from v3.6.2)


3.6.2
-----

* Fixed handling of :func:`~functools.partial` wrapped coroutine functions in ``AsyncIOExecutor``
  and ``TornadoExecutor`` (PR by shipmints)


3.6.1
-----

* Fixed OverflowError on Qt scheduler when the wait time is very long
* Fixed methods inherited from base class could not be executed by processpool executor
  (PR by Yang Jian)


3.6.0
-----

* Adapted ``RedisJobStore`` to v3.0 of the ``redis`` library
* Adapted ``RethinkDBJobStore`` to v2.4 of the ``rethink`` library
* Fixed ``DeprecationWarnings`` about ``collections.abc`` on Python 3.7 (PR by Roman Levin)


3.5.3
-----

* Fixed regression introduced in 3.5.2: Class methods were mistaken for instance methods and thus
  were broken during serialization
* Fixed callable name detection for methods in old style classes


3.5.2
-----

* Fixed scheduling of bound methods on persistent job stores (the workaround of scheduling
  ``YourClass.methodname`` along with an explicit ``self`` argument is no longer necessary as this
  is now done automatically for you)
* Added the FAQ section to the docs
* Made ``BaseScheduler.start()`` raise a ``RuntimeError`` if running under uWSGI with threads
  disabled


3.5.1
-----

* Fixed ``OverflowError`` on Windows when the wait time is too long

* Fixed ``CronTrigger`` sometimes producing fire times beyond ``end_date`` when jitter is enabled
  (thanks to gilbsgilbs for the tests)

* Fixed ISO 8601 UTC offset information being silently discarded from string formatted datetimes by
  adding support for parsing them


3.5.0
-----

* Added the ``engine_options`` option to ``SQLAlchemyJobStore``

* Added the ``jitter`` options to ``IntervalTrigger`` and ``CronTrigger`` (thanks to gilbsgilbs)

* Added combining triggers (``AndTrigger`` and ``OrTrigger``)

* Added better validation for the steps and ranges of different expressions in ``CronTrigger``

* Added support for named months (``jan`` – ``dec``) in ``CronTrigger`` month expressions

* Added support for creating a ``CronTrigger`` from a crontab expression

* Allowed spaces around commas in ``CronTrigger`` fields

* Fixed memory leak due to a cyclic reference when jobs raise exceptions
  (thanks to gilbsgilbs for help on solving this)

* Fixed passing ``wait=True`` to ``AsyncIOScheduler.shutdown()`` (although it doesn't do much)

* Cancel all pending futures when ``AsyncIOExecutor`` is shut down


3.4.0
-----

* Dropped support for Python 3.3

* Added the ability to specify the table schema for ``SQLAlchemyJobStore``
  (thanks to Meir Tseitlin)

* Added a workaround for the ``ImportError`` when used with PyInstaller and the likes
  (caused by the missing packaging metadata when APScheduler is packaged with these tools)


3.3.1
-----

* Fixed Python 2.7 compatibility in ``TornadoExecutor``


3.3.0
-----

* The asyncio and Tornado schedulers can now run jobs targeting coroutine functions
  (requires Python 3.5; only native coroutines (``async def``) are supported)

* The Tornado scheduler now uses TornadoExecutor as its default executor (see above as for why)

* Added ZooKeeper job store (thanks to Jose Ignacio Villar for the patch)

* Fixed job store failure (``get_due_jobs()``) causing the scheduler main loop to exit (it now
  waits a configurable number of seconds before retrying)

* Fixed ``@scheduled_job`` not working when serialization is required (persistent job stores and
  ``ProcessPoolScheduler``)

* Improved import logic in ``ref_to_obj()`` to avoid errors in cases where traversing the path with
  ``getattr()`` would not work (thanks to Jarek Glowacki for the patch)

* Fixed CronTrigger's weekday position expressions failing on Python 3

* Fixed CronTrigger's range expressions sometimes allowing values outside the given range


3.2.0
-----

* Added the ability to pause and unpause the scheduler

* Fixed pickling problems with persistent jobs when upgrading from 3.0.x

* Fixed AttributeError when importing apscheduler with setuptools < 11.0

* Fixed some events missing from ``apscheduler.events.__all__`` and
  ``apscheduler.events.EVENTS_ALL``

* Fixed wrong run time being set for date trigger when the timezone isn't the same as the local one

* Fixed builtin ``id()`` erroneously used in MongoDBJobStore's ``JobLookupError()``

* Fixed endless loop with CronTrigger that may occur when the computer's clock resolution is too
   low (thanks to Jinping Bai for the patch)


3.1.0
-----

* Added RethinkDB job store (contributed by Allen Sanabria)

* Added method chaining to the ``modify_job()``, ``reschedule_job()``, ``pause_job()`` and
   ``resume_job()`` methods in ``BaseScheduler`` and the corresponding methods in the ``Job`` class

* Added the EVENT_JOB_SUBMITTED event that indicates a job has been submitted to its executor.

* Added the EVENT_JOB_MAX_INSTANCES event that indicates a job's execution was skipped due to its
  maximum number of concurrently running instances being reached

* Added the time zone to the  repr() output of ``CronTrigger`` and ``IntervalTrigger``

* Fixed rare race condition on scheduler ``shutdown()``

* Dropped official support for CPython 2.6 and 3.2 and PyPy3

* Moved the connection logic in database backed job stores to the ``start()`` method

* Migrated to setuptools_scm for versioning

* Deprecated the various version related variables in the ``apscheduler`` module
  (``apscheduler.version_info``, ``apscheduler.version``, ``apscheduler.release``,
  ``apscheduler.__version__``)


3.0.6
-----

* Fixed bug in the cron trigger that produced off-by-1-hour datetimes when crossing the daylight
  saving threshold (thanks to Tim Strazny for reporting)


3.0.5
-----

* Fixed cron trigger always coalescing missed run times into a single run time
  (contributed by Chao Liu)

* Fixed infinite loop in the cron trigger when an out-of-bounds value was given in an expression

* Fixed debug logging displaying the next wakeup time in the UTC timezone instead of the
  scheduler's configured timezone

* Allowed unicode function references in Python 2


3.0.4
-----

* Fixed memory leak in the base executor class (contributed by Stefan Nordhausen)


3.0.3
-----

* Fixed compatibility with pymongo 3.0


3.0.2
-----

* Fixed ValueError when the target callable has a default keyword argument that wasn't overridden

* Fixed wrong job sort order in some job stores

* Fixed exception when loading all jobs from the redis job store when there are paused jobs in it

* Fixed AttributeError when printing a job list when there were pending jobs

* Added setuptools as an explicit requirement in install requirements


3.0.1
-----

* A wider variety of target callables can now be scheduled so that the jobs are still serializable
  (static methods on Python 3.3+, unbound methods on all except Python 3.2)

* Attempting to serialize a non-serializable Job now raises a helpful exception during
  serialization. Thanks to Jeremy Morgan for pointing this out.

* Fixed table creation with SQLAlchemyJobStore on MySQL/InnoDB

* Fixed start date getting set too far in the future with a timezone different from the local one

* Fixed _run_job_error() being called with the incorrect number of arguments in most executors


3.0.0
-----

* Added support for timezones (special thanks to Curtis Vogt for help with this one)

* Split the old Scheduler class into BlockingScheduler and BackgroundScheduler and added
  integration for asyncio (PEP 3156), Gevent, Tornado, Twisted and Qt event loops

* Overhauled the job store system for much better scalability

* Added the ability to modify, reschedule, pause and resume jobs

* Dropped the Shelve job store because it could not work with the new job store system

* Dropped the max_runs option and run counting of jobs since it could not be implemented reliably

* Adding jobs is now done exclusively through ``add_job()`` -- the shortcuts to triggers were
  removed

* Added the ``end_date`` parameter to cron and interval triggers

* It is now possible to add a job directly to an executor without scheduling, by omitting the
  trigger argument

* Replaced the thread pool with a pluggable executor system

* Added support for running jobs in subprocesses (via the ``processpool`` executor)

* Switched from nose to py.test for running unit tests


2.1.0
-----

* Added Redis job store

* Added a "standalone" mode that runs the scheduler in the calling thread

* Fixed disk synchronization in ShelveJobStore

* Switched to PyPy 1.9 for PyPy compatibility testing

* Dropped Python 2.4 support

* Fixed SQLAlchemy 0.8 compatibility in SQLAlchemyJobStore

* Various documentation improvements


2.0.3
-----

* The scheduler now closes the job store that is being removed, and all job stores on shutdown() by
  default

* Added the ``last`` expression in the day field of CronTrigger (thanks rcaselli)

* Raise a TypeError when fields with invalid names are passed to CronTrigger (thanks Christy
  O'Reilly)

* Fixed the persistent.py example by shutting down the scheduler on Ctrl+C

* Added PyPy 1.8 and CPython 3.3 to the test suite

* Dropped PyPy 1.4 - 1.5 and CPython 3.1 from the test suite

* Updated setup.cfg for compatibility with distutils2/packaging

* Examples, documentation sources and unit tests are now packaged in the source distribution


2.0.2
-----

* Removed the unique constraint from the "name" column in the SQLAlchemy
  job store

* Fixed output from Scheduler.print_jobs() which did not previously output
  a line ending at the end


2.0.1
-----

* Fixed cron style jobs getting wrong default values


2.0.0
-----

* Added configurable job stores with several persistent back-ends
  (shelve, SQLAlchemy and MongoDB)

* Added the possibility to listen for job events (execution, error, misfire,
  finish) on a scheduler

* Added an optional start time for cron-style jobs

* Added optional job execution coalescing for situations where several
  executions of the job are due

* Added an option to limit the maximum number of concurrently executing
  instances of the job

* Allowed configuration of misfire grace times on a per-job basis

* Allowed jobs to be explicitly named

* All triggers now accept dates in string form (YYYY-mm-dd HH:MM:SS)

* Jobs are now run in a thread pool; you can either supply your own PEP 3148
  compliant thread pool or let APScheduler create its own

* Maximum run count can be configured for all jobs, not just those using
  interval-based scheduling

* Fixed a v1.x design flaw that caused jobs to be executed twice when the
  scheduler thread was woken up while still within the allowable range of their
  previous execution time (issues #5, #7)

* Changed defaults for cron-style jobs to be more intuitive -- it will now
  default to all minimum values for fields lower than the least significant
  explicitly defined field


1.3.1
-----

* Fixed time difference calculation to take into account shifts to and from
  daylight saving time


1.3.0
-----

* Added __repr__() implementations to expressions, fields, triggers, and jobs
  to help with debugging

* Added the dump_jobs method on Scheduler, which gives a helpful listing of
  all jobs scheduled on it

* Fixed positional weekday (3th fri etc.) expressions not working except in
  some edge cases (fixes #2)

* Removed autogenerated API documentation for modules which are not part of
  the public API, as it might confuse some users

.. Note:: Positional weekdays are now used with the **day** field, not
   **weekday**.


1.2.1
-----

* Fixed regression: add_cron_job() in Scheduler was creating a CronTrigger with
  the wrong parameters (fixes #1, #3)

* Fixed: if the scheduler is restarted, clear the "stopped" flag to allow
  jobs to be scheduled again


1.2.0
-----

* Added the ``week`` option for cron schedules

* Added the ``daemonic`` configuration option

* Fixed a bug in cron expression lists that could cause valid firing times
  to be missed

* Fixed unscheduling bound methods via unschedule_func()

* Changed CronTrigger constructor argument names to match those in Scheduler


1.01
----

* Fixed a corner case where the combination of hour and day_of_week parameters
  would cause incorrect timing for a cron trigger
