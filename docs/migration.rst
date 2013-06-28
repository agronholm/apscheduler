Migrating from APScheduler v2.x to 3.0
======================================

The 3.0 series is API incompatible with previous releases due to a design
overhaul.

API changes
-----------

* The trigger-specific scheduling methods have been removed entirely from the
  Scheduler class. Instead, you have to use the
  :meth:`~apscheduler.scheduler.Scheduler.add_job` method or the
  :meth:`~apscheduler.scheduler.Scheduler.scheduled_job` decorator, giving the
  entry point name of the trigger, or an already constructed instance.
  It should also be noted that the signature of
  :meth:`~apscheduler.scheduler.Scheduler.add_job` has changed due to this.
* Adding a job before the scheduler has been started can now fail if no next
  execution time can be calculated for it. Previously it would only fail when
  the scheduler was started.
* The "simple" trigger has been renamed to "date"


Migrating from APScheduler v1.x to 2.0
======================================

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
