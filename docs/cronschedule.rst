Cron-style scheduling
=====================

This is the most powerful scheduling method available in APScheduler.
You can specify a variety of different expressions on each field, and
when determining the next execution time, it finds the earliest possible
time that satisfies the conditions in every field.
This behavior closely resembles the "Cron" utility found in most UNIX-like
operating systems.

Expression types
----------------

The following table lists all the available expressions
applicable in cron-style schedules.

============ ========= ======================================================
Expression   Field     Description
============ ========= ======================================================
``*``        any       Fire on every value
``*/a``      any       Fire every ``a`` values, starting from the minimum
``a-b``      any       Fire on any value within the ``a-b`` range
                       (a must be smaller than b)
``a-b/c``    any       Fire every ``c`` values within the ``a-b`` range
``xth y``    weekdays  Fire on the ``x`` -th occurrence of weekday ``y`` within
                       a month
``last x``   weekdays  Fire on last weekday ``x``
``x,y,z``    any       Fire on any matching expression; can combine any number
                       of any of the above expressions
============ ========= ======================================================

.. Note::

	Weekdays are numbered from 0-6, where Monday is 0 and Sunday is 6.
	The corresponding textual values are "mon", "tue", "wed", "thu", "fri", "sat"
	and "sun". You cannot combine textual values and numeric values, and you cannot
	use the stepping (``a-b/c``) syntax with the textual values.


Example
-------

::

    from apscheduler.scheduler import Scheduler
    
    # Start the scheduler
    sched = Scheduler()
    sched.start()

    def job_function():
        print "Hello World"

    # Schedules job_function to be run on the third Friday
    # of June, July, August, November and December, from midnight
    # to 3 am
    sched.add_cron_job(job_function, month='6-8,11-12', day_of_week='3rd fri',
                       hour='0-3')

Decorator syntax
----------------

As a convenience, there is an alternative syntax for using cron-style
schedules. The :meth:`~apscheduler.scheduler.Scheduler.cron_schedule`
decorator can be attached to any function, and has the same syntax as
:meth:`~apscheduler.scheduler.Scheduler.add_cron_job`, except for the ``func``
parameter, obviously.

::

    @sched.cron_schedule(day_of_week='last sun')
    def some_decorated_task():
        print "I am printed on the last sunday of every month!"