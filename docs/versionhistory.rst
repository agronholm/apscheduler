Version history
===============

To find out how to migrate your application from a previous version of
APScheduler, see the :doc:`migration section <migration>`.

3.1.0
-----

* Added RethinkDB job store (contributed by Allen Sanabria)


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

* Attempting to serialize a non-serializable Job now raises a helpful exception during serialization.
  Thanks to Jeremy Morgan for pointing this out.

* Fixed table creation with SQLAlchemyJobStore on MySQL/InnoDB

* Fixed start date getting set too far in the future with a timezone different from the local one

* Fixed _run_job_error() being called with the incorrect number of arguments in most executors


3.0.0
-----

* Added support for timezones (special thanks to Curtis Vogt for help with this one)

* Split the old Scheduler class into BlockingScheduler and BackgroundScheduler and added integration for
  asyncio (PEP 3156), Gevent, Tornado, Twisted and Qt event loops

* Overhauled the job store system for much better scalability

* Added the ability to modify, reschedule, pause and resume jobs

* Dropped the Shelve job store because it could not work with the new job store system

* Dropped the max_runs option and run counting of jobs since it could not be implemented reliably

* Adding jobs is now done exclusively through ``add_job()`` -- the shortcuts to triggers were removed

* Added the ``end_date`` parameter to cron and interval triggers

* It is now possible to add a job directly to an executor without scheduling, by omitting the trigger argument

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

* The scheduler now closes the job store that is being removed, and all job stores on shutdown() by default

* Added the ``last`` expression in the day field of CronTrigger (thanks rcaselli)

* Raise a TypeError when fields with invalid names are passed to CronTrigger (thanks Christy O'Reilly)

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

* Added an option to limit the maximum number of concurrenctly executing
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
