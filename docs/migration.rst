###############################################
Migrating from previous versions of APScheduler
###############################################

.. py:currentmodule:: apscheduler

From v3.x to v4.0
=================

APScheduler 4.0 has undergone a partial rewrite since the 3.x series.

There is currently no way to automatically import schedules from a persistent 3.x job
store, but this shortcoming will be rectified before the final v4.0 release.

Terminology and architectural design changes
--------------------------------------------

The concept of a *job* has been split into :class:`Task`, :class:`Schedule` and
:class:`Job`. See the documentation of each class (and read the tutorial) to understand
their roles.

**Executors** have been replaced by *workers*. Workers were designed to be able to run
independently from schedulers. Workers now *pull* jobs from the data store instead of
the scheduler pushing jobs directly to them.

**Data stores**, previously called *job stores*, have been redesigned to work with
multiple running schedulers and workers, both for purposes of scalability and fault
tolerance. Many data store implementations were dropped because they were either too
burdensome to support, or the backing services were not sophisticated enough to handle
the increased requirements.

**Event brokers** are a new component in v4.0. They relay events between schedulers and
workers, enabling them to work together with a shared data store. External (as opposed
to local) event broker services are required in multi-node or multi-process deployment
scenarios.

**Triggers** are now stateful. This change was found to be necessary to properly support
combining triggers (:class:`~.triggers.combining.AndTrigger` and
:class:`~.triggers.combining.OrTrigger`), as they needed to keep track of the next run
times of all the triggers contained within. This change also enables some more
sophisticated custom trigger implementations.

**Time zone** support has been revamped to use :mod:`zoneinfo` (or `backports.zoneinfo`_
on Python versions earlier than 3.9) zones instead of pytz zones. You should not use
pytz with APScheduler anymore.

`Entry points`_ are no longer used or supported, as they were more trouble than they
were worth, particularly with packagers like py2exe or PyInstaller which by default did
not package distribution metadata. Thus, triggers and data stores have to be explicitly
instantiated.

.. _backports.zoneinfo: https://pypi.org/project/backports.zoneinfo/
.. _Entry points: https://packaging.python.org/en/latest/specifications/entry-points/

Scheduler changes
-----------------

The ``add_job()`` method is now :meth:`~Scheduler.add_schedule`. The scheduler still has
a method named :meth:`~Scheduler.add_job`, but this is meant for making one-off runs of a
task. Previously you would have had to call ``add_job()`` with a
:class:`~apscheduler.triggers.date.DateTrigger` using the current time as the run time.

The two most commonly used schedulers, ``BlockingScheduler`` and
``BackgroundScheduler``, have often caused confusion among users and have thus been
combined into :class:`~.schedulers.sync.Scheduler`. This new unified scheduler class
has two methods that replace the ``start()`` method used previously:
:meth:`~.schedulers.sync.Scheduler.run_until_stopped` and
:meth:`~.schedulers.sync.Scheduler.start_in_background`. The former should be used if
you previously used ``BlockingScheduler``, and the latter if you used
``BackgroundScheduler``.

The asyncio scheduler has been replaced with a more generic :class:`AsyncScheduler`,
which is based on AnyIO_ and thus also supports Trio_ in addition to :mod:`asyncio`.
The API of the async scheduler differs somewhat from its synchronous counterpart. In
particular, it **requires** itself to be used as an async context manager – whereas with
the synchronous scheduler, use as a context manager is recommended but not required.

All other scheduler implementations have been dropped because they were either too
burdensome to support, or did not seem necessary anymore. Some of the dropped
implementations (particularly Qt) are likely to be re-added before v4.0 final.

Schedulers no longer support multiple data stores. If you need this capability, you
should run multiple schedulers instead.

Configuring and running the scheduler has been radically simplified. The ``configure()``
method is gone, and all configuration is now passed as keyword arguments to the
scheduler class.

.. _AnyIO: https://pypi.org/project/anyio/
.. _Trio: https://pypi.org/project/trio/

Trigger changes
---------------

As the scheduler is no longer used to create triggers, any supplied datetimes will be
assumed to be in the local time zone. If you wish to change the local time zone, you
should set the ``TZ`` environment variable to either the name of the desired timezone
(e.g. ``Europe/Helsinki``) or to a path of a time zone file. See the tzlocal_
documentation for more information.

**Jitter** support has been moved from individual triggers to the schedule level.
This not only simplified trigger design, but also enabled the scheduler to provide
information about the randomized jitter and the original run time to the user.

:class:`~.triggers.cron.CronTrigger` was changed to respect the standard order of
weekdays, so that Sunday is now 0 and Saturday is 6. If you used numbered weekdays
before, you must change your trigger configuration to match. If in doubt, use
abbreviated weekday names (e.g. ``sun``, ``fri``) instead.

:class:`~.triggers.interval.IntervalTrigger` was changed to start immediately, instead
of waiting for the first interval to pass. If you have workarounds in place to "fix"
the previous behavior, you should remove them.

.. _tzlocal: https://pypi.org/project/tzlocal/

From v3.0 to v3.2
=================

Prior to v3.1, the scheduler inadvertently exposed the ability to fetch and manipulate jobs before
the scheduler had been started. The scheduler now requires you to call ``scheduler.start()`` before
attempting to access any of the jobs in the job stores. To ensure that no old jobs are mistakenly
executed, you can start the scheduler in paused mode (``scheduler.start(paused=True)``) (introduced
in v3.2) to avoid any premature job processing.


From v2.x to v3.0
=================

The 3.0 series is API incompatible with previous releases due to a design overhaul.

Scheduler changes
-----------------

* The concept of "standalone mode" is gone. For ``standalone=True``, use
  :class:`~apscheduler.schedulers.blocking.BlockingScheduler` instead, and for
  ``standalone=False``, use :class:`~apscheduler.schedulers.background.BackgroundScheduler`.
  BackgroundScheduler matches the old default semantics.
* Job defaults (like ``misfire_grace_time`` and ``coalesce``) must now be passed in a dictionary as
  the ``job_defaults`` option to :meth:`~apscheduler.schedulers.base.BaseScheduler.configure`. When
  supplying an ini-style configuration as the first argument, they will need a corresponding
  ``job_defaults.`` prefix.
* The configuration key prefix for job stores was changed from ``jobstore.`` to ``jobstores.`` to
  match the dict-style configuration better.
* The ``max_runs`` option has been dropped since the run counter could not be reliably preserved
  when replacing a job with another one with the same ID. To make up for this, the ``end_date``
  option was added to cron and interval triggers.
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
* :meth:`~apscheduler.scheduler.Scheduler.unschedule_job` and
  :meth:`~apscheduler.scheduler.Scheduler.unschedule_func` have been replaced by
  :meth:`~apscheduler.schedulers.base.BaseScheduler.remove_job`. You can also unschedule a job by
  using the job handle returned from :meth:`~apscheduler.schedulers.base.BaseScheduler.add_job`.

Job store changes
-----------------

The job store system was completely overhauled for both efficiency and forwards compatibility.
Unfortunately, this means that the old data is not compatible with the new job stores.
If you need to migrate existing data from APScheduler 2.x to 3.x, contact the APScheduler author.

The Shelve job store had to be dropped because it could not support the new job store design.
Use SQLAlchemyJobStore with SQLite instead.

Trigger changes
---------------

From 3.0 onwards, triggers now require a pytz timezone. This is normally provided by the scheduler,
but if you were instantiating triggers manually before, then one must be supplied as the
``timezone`` argument.

The only other backwards incompatible change was that ``get_next_fire_time()`` takes two arguments
now: the previous fire time and the current datetime.


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
