###############################################
Migrating from previous versions of APScheduler
###############################################

From v2.x to 3.0
================

The 3.0 series is API incompatible with previous releases due to a design overhaul.

Scheduler changes
-----------------

* The concept of "standalone mode" is gone. For ``standalone=True``, use
  :class:`~apscheduler.schedulers.blocking.BlockingScheduler` instead, and for ``standalone=False``, use
  :class:`~apscheduler.schedulers.background.BackgroundScheduler`. BackgroundScheduler matches the old default
  semantics.
* Job defaults (like ``misfire_grace_time`` and ``coalesce``) must now be passed in a dictionary as the
  ``job_defaults`` option to :meth:`~apscheduler.schedulers.base.BaseScheduler.configure`. When supplying an ini-style
  configuration as the first argument, they will need a corresponding ``job_defaults.`` prefix.
* The configuration key prefix for job stores was changed from ``jobstore.`` to ``jobstores.`` to match the dict-style
  configuration better.
* The ``max_runs`` option has been dropped since the run counter could not be reliably preserved when replacing a job
  with another one with the same ID. To make up for this, the ``end_date`` option was added to cron and interval
  triggers.
* The old thread pool is gone, replaced by ``ThreadPoolExecutor``.
  This means that the old ``threadpool`` options are no longer valid.
  See :ref:`scheduler-config` on how to configure executors.
* The trigger-specific scheduling methods have been removed entirely from the scheduler.
  Use the generic :meth:`~apscheduler.schedulers.base.BaseScheduler.add_job` method or the
  :meth:`~apscheduler.schedulers.base.BaseScheduler.scheduled_job` decorator instead.
  The signatures of these methods were changed significantly.
* The ``shutdown_threadpool`` and ``close_jobstores`` options have been removed from the
  :meth:`~apscheduler.schedulers.base.BaseScheduler.shutdown` method.
  Executors and job stores are now always shut down on scheduler shutdown.
* :meth:`~apscheduler.scheduler.Scheduler.unschedule_job` and :meth:`~apscheduler.scheduler.Scheduler.unschedule_func`
  have been replaced by :meth:`~apscheduler.schedulers.base.BaseScheduler.remove_job`.
  You can also unschedule a job by using the job handle returned from
  :meth:`~apscheduler.schedulers.base.BaseScheduler.add_job`.

Job store changes
-----------------

The job store system was completely overhauled for both efficiency and forwards compatibility.
Unfortunately, this means that the old data is not compatible with the new job stores.
If you need to migrate existing data from APScheduler 2.x to 3.x, contact the APScheduler author.

The Shelve job store had to be dropped because it could not support the new job store design.
Use SQLAlchemyJobStore with SQLite instead.

Trigger changes
---------------

From 3.0 onwards, triggers now require a pytz timezone. This is normally provided by the scheduler, but if you were
instantiating triggers manually before, then one must be supplied as the ``timezone`` argument.

The only other backwards incompatible change was that ``get_next_fire_time()`` takes two arguments now: the previous
fire time and the current datetime.


From v1.x to 2.0
================

There have been some API changes since the 1.x series. This document
explains the changes made to v2.0 that are incompatible with the v1.x API.

API changes
-----------

* The behavior of cron scheduling with regards to default values for omitted
  fields has been made more intuitive -- omitted fields lower than the least
  significant explicitly defined field will default to their minimum values
  except for the week number and weekday fields
* SchedulerShutdownError has been removed -- jobs are now added tentatively
  and scheduled for real when/if the scheduler is restarted
* Scheduler.is_job_active() has been removed -- use
  ``job in scheduler.get_jobs()`` instead
* dump_jobs() is now print_jobs() and prints directly to the given file or
  sys.stdout if none is given
* The ``repeat`` parameter was removed from
  :meth:`~apscheduler.scheduler.Scheduler.add_interval_job` and
  :meth:`~apscheduler.scheduler.Scheduler.interval_schedule` in favor of the
  universal ``max_runs`` option
* :meth:`~apscheduler.scheduler.Scheduler.unschedule_func` now raises a
  KeyError if the given function is not scheduled
* The semantics of :meth:`~apscheduler.scheduler.Scheduler.shutdown` have
  changed -- the method no longer accepts a numeric argument, but two booleans


Configuration changes
---------------------

* The scheduler can no longer be reconfigured while it's running
