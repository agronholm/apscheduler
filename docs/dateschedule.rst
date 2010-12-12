Simple date-based scheduling
============================

This is the simplest possible method of scheduling a job.
It schedules a job to be executed once at the specified time.
This is the in-process equivalent to the UNIX "at" command.

::

    from datetime import date
    from apscheduler.scheduler import Scheduler
    
    # Start the scheduler
    sched = Scheduler()
    sched.start()
    
    # Define the function that is to be executed
    def my_job(text):
        print text
    
    # The job will be executed on November 6th, 2009
    exec_date = date(2009, 11, 6)
	
    # Store the job in a variable in case we want to cancel it
    job = sched.add_date_job(my_job, exec_date, ['text'])

We could be more specific with the scheduling too::

    from datetime import datetime

    # The job will be executed on November 6th, 2009 at 16:30:05
    job = sched.add_date_job(my_job, datetime(2009, 11, 6, 16, 30, 5), ['text'])

You can even specify a date as text, with or without the time part::

    job = sched.add_date_job(my_job, '2009-11-06 16:30:05', ['text'])

    # Even down to the microsecond level, if you really want to!
    job = sched.add_date_job(my_job, '2009-11-06 16:30:05.720400', ['text'])
