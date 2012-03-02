Cron-style scheduling
=====================

This is the most powerful scheduling method available in APScheduler.
You can specify a variety of different expressions on each field, and
when determining the next execution time, it finds the earliest possible
time that satisfies the conditions in every field.
This behavior resembles the "Cron" utility found in most UNIX-like operating
systems.

You can also specify the starting date for the cron-style schedule through the
``start_date`` parameter, which can be given as a date/datetime object or text.
See the :doc:`Date-based scheduling section <dateschedule>` for examples on
that.

Unlike with crontab expressions, you can omit fields that you don't need.
Fields greater than the least significant explicitly defined field default to
``*`` while lesser fields default to their minimum values except for ``week``
and ``day_of_week`` which default to ``*``. For example, if you specify only
``day=1, minute=20``, then the job will execute on the first day of every month
on every year at 20 minutes of every hour. The code examples below should
further illustrate this behavior.

.. Note:: The behavior for omitted fields was changed in APScheduler 2.0.
          Omitted fields previously always defaulted to ``*``.


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
``last``     day       Fire on the last day within the month
``x,y,z``    any       Fire on any matching expression; can combine any number
                       of any of the above expressions
============ ========= ======================================================


Example 1
---------

::

    from apscheduler.scheduler import Scheduler
    
    # Start the scheduler
    sched = Scheduler()
    sched.start()

    def job_function():
        print "Hello World"

    # Schedules job_function to be run on the third Friday
    # of June, July, August, November and December at 00:00, 01:00, 02:00 and 03:00
    sched.add_cron_job(job_function, month='6-8,11-12', day='3rd fri', hour='0-3')


Example 2
---------

::

    # Initialization similar as above, the backup function defined elsewhere
    
    # Schedule a backup to run once from Monday to Friday at 5:30 (am)
    sched.add_cron_job(backup, day_of_week='mon-fri', hour=5, minute=30)


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
        print "I am printed at 00:00:00 on the last Sunday of every month!"

If you need to unschedule the decorated functions, you can do it this way::

    scheduler.unschedule_job(job_function.job)
