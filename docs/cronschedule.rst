Cron-style scheduling
=====================

This is the most powerful scheduling method available in APScheduler.
You can specify a variety of different expressions on each field, and
when determining the next execution time, it finds the earliest possible
time that satisfies the conditions in every field.
This behavior closely resembles the "Cron" utility found in most UNIX-like
operating systems.

Available fields
----------------

=============== ======================================================
Field           Description
=============== ======================================================
``year``        4-digit year number
``month``       month number (1-12)
``day``         day of the month (1-31)
``week``        ISO week number (1-53)
``day_of_week`` number or name of weekday (0-6 or mon,tue,wed,thu,fri,sat,sun)
``hour``        hour (0-23)
``minute``      minute (0-59)
``second``      second (0-59)
=============== ======================================================

.. Note:: The first weekday is always **monday**.

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
``xth y``    day       Fire on the ``x`` -th occurrence of weekday ``y`` within
                       the month
``last x``   day       Fire on the last occurrence of weekday ``x`` within the
                       month
``x,y,z``    any       Fire on any matching expression; can combine any number
                       of any of the above expressions
============ ========= ======================================================

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
    sched.add_cron_job(job_function, month='6-8,11-12', day='3rd fri',
                       hour='0-3')

Decorator syntax
----------------

As a convenience, there is an alternative syntax for using cron-style
schedules. The :meth:`~apscheduler.scheduler.Scheduler.cron_schedule`
decorator can be attached to any function, and has the same syntax as
:meth:`~apscheduler.scheduler.Scheduler.add_cron_job`, except for the ``func``
parameter, obviously.

::

    @sched.cron_schedule(day='last sun')
    def some_decorated_task():
        print "I am printed on the last sunday of every month!"
