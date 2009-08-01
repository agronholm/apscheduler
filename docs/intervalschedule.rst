Interval-based scheduling
=========================

This method schedules jobs to be run on selected intervals.
The execution of the job starts after the given delay, or on
``start_date`` if specified. After that, the job will be executed
again after the specified delay. The total number of times the job
will be executed depends on the ``repeat`` parameter.
The value of ``repeat`` is by default 0, which means that the job is
repeated until the scheduler is shut down.

::

    from apscheduler.scheduler import Scheduler
    
    # Start the scheduler
    sched = Scheduler()
    sched.start()
    
    def job_function():
        print "Hello World"

    # Schedule job_function to be called every two hours
    sched.add_interval_job(job_function, hours=2)

Decorator syntax
----------------

As a convenience, there is an alternative syntax for using interval-based
schedules. The :meth:`~apscheduler.Scheduler.interval_schedule` decorator can be
attached to any function, and has the same syntax as
:meth:`~apscheduler.Scheduler.add_interval_job`, except for the ``func``
parameter, obviously.

::

    from apscheduler.scheduler import Scheduler
    
    # Start the scheduler
    sched = Scheduler()
    sched.start()
    
    # Schedule job_function to be called every two hours
    @sched.interval_schedule(hours=2)
    def job_function():
        print "Hello World"
