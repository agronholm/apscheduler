Extending APScheduler
=====================

This document is meant to explain how to add extra functionality to
APScheduler, such as custom triggers or job stores.


Writing and using custom triggers
---------------------------------

Triggers determine the times when the jobs should be run.
APScheduler comes with three built-in triggers --
:class:`~apscheduler.triggers.simple.SimpleTrigger`,
:class:`~apscheduler.triggers.interval.IntervalTrigger` and
:class:`~apscheduler.triggers.cron.CronTrigger`. You don't normally use these
directly, since the scheduler has shortcut methods for these built-in
triggers.

If you need to use some specialized scheduling algorithm, you can implement
that as a custom trigger class. The only method a trigger class has to
implement is ``get_next_fire_time``. This method receives a starting date
(a :class:`~datetime.datetime` object) as its sole argument. It should return
the next time the trigger will fire (starting from and including the given time),
according to whatever scheduling logic you wish to implement. If no such
datetime can be computed, it should return ``None``.

To schedule a job using your custom trigger, you can either extends the 
:class:`~apscheduler.scheduler.Scheduler` class to include your own shortcuts,
or use the generic :meth:`~apscheduler.scheduler.Scheduler.add_job` method to
add your jobs.


Writing and using custom job stores
-----------------------------------

Job store classes should preferably inherit from
:class:`apscheduler.jobstores.base.JobStore`. This class provides stubbed out
methods which any implementation should override. These methods also contain
useful documentation regarding the responsibilities of a job store. It is
recommended that you look at the existing job store implementations for
examples.

To use your job store, you must add it to the scheduler as normal::

  jobstore = MyJobStore()
  scheduler.add_jobstore(jobstore, 'mystore')
