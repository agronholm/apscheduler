Extending APScheduler
=====================

This document is meant to explain how to add extra functionality to
APScheduler, such as custom triggers or job stores.


Writing and using custom triggers
---------------------------------

If you need to use some specialized scheduling algorithm, you can implement
that as a custom trigger class. The only method a trigger class has to
implement is ``get_next_fire_time``. This method receives a starting date
(a :class:``~datetime.datetime`` object) as its sole argument, and the method
should return the next time the trigger will fire, according to whatever
scheduling logic you wish to implement. If no such date/time can be computed,
it should return ``None``.

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
  scheduler.add_jobstore(jobstore)

If you do not specify an explicit alias when adding the job store, it will be
automatically determined from the class name. The class name is converted to
lower case and the "jobstore" suffix is removed. Thus, for "MyJobStore", the
alias would be "my". You can override the default behavior by setting the
``default_alias`` variable in your job store.
