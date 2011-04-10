Interval-based scheduling
=========================

This method schedules jobs to be run on selected intervals. The execution of
the job starts after the given delay, or on ``start_date`` if specified. After
that, the job will be executed again after the specified delay. The
``start_date`` parameter can be given as a date/datetime object or text. See
the :doc:`Date-based scheduling section <dateschedule>` for more examples on
that.

::

    from datetime import datetime

    from apscheduler.scheduler import Scheduler
    
    # Start the scheduler
    sched = Scheduler()
    sched.start()
    
    def job_function():
        print "Hello World"

    # Schedule job_function to be called every two hours
    sched.add_interval_job(job_function, hours=2)

    # The same as before, but start after a certain time point
    sched.add_interval_job(job_function, hours=2, start_date='2010-10-10 09:30')


Decorator syntax
----------------

As a convenience, there is an alternative syntax for using interval-based
schedules. The :meth:`~apscheduler.scheduler.Scheduler.interval_schedule`
decorator can be attached to any function, and has the same syntax as
:meth:`~apscheduler.scheduler.Scheduler.add_interval_job`, except for the
``func`` parameter, obviously.

::

    from apscheduler.scheduler import Scheduler
    
    # Start the scheduler
    sched = Scheduler()
    sched.start()
    
    # Schedule job_function to be called every two hours
    @sched.interval_schedule(hours=2)
    def job_function():
        print "Hello World"

If you need to unschedule the decorated functions, you can do it this way::

    scheduler.unschedule_job(job_function.job)
