:mod:`apscheduler.triggers.cron`
================================

.. automodule:: apscheduler.triggers.cron

API
---

Trigger alias for :meth:`~apscheduler.schedulers.base.BaseScheduler.add_job`: ``cron``

.. autoclass:: CronTrigger
    :show-inheritance:


Introduction
------------

This is the most powerful of the built-in triggers in APScheduler. You can specify a variety of different expressions
on each field, and when determining the next execution time, it finds the earliest possible time that satisfies the
conditions in every field. This behavior resembles the "Cron" utility found in most UNIX-like operating systems.

You can also specify the starting date and ending dates for the cron-style schedule through the ``start_date`` and
``end_date`` parameters, respectively. They can be given as a date/datetime object or text (in the
`ISO 8601 <https://en.wikipedia.org/wiki/ISO_8601>`_ format).

Unlike with crontab expressions, you can omit fields that you don't need. Fields greater than the least significant
explicitly defined field default to ``*`` while lesser fields default to their minimum values except for ``week`` and
``day_of_week`` which default to ``*``. For example, ``day=1, minute=20`` is equivalent to
``year='*', month='*', day=1, week='*', day_of_week='*', hour='*', minute=20, second=0``. The job will then execute
on the first day of every month on every year at 20 minutes of every hour. The code examples below should further
illustrate this behavior.

.. note:: The behavior for omitted fields was changed in APScheduler 2.0.
          Omitted fields previously always defaulted to ``*``.


Expression types
----------------

The following table lists all the available expressions for use in the fields from year to second.
Multiple expression can be given in a single field, separated by commas.

============== ===== =======================================================================================
Expression     Field Description
============== ===== =======================================================================================
``*``          any   Fire on every value
``*/a``        any   Fire every ``a`` values, starting from the minimum
``a-b``        any   Fire on any value within the ``a-b`` range (a must be smaller than b)
``a-b/c``      any   Fire every ``c`` values within the ``a-b`` range
``xth y``      day   Fire on the ``x`` -th occurrence of weekday ``y`` within the month
``last x``     day   Fire on the last occurrence of weekday ``x`` within the month
``last``       day   Fire on the last day within the month
``x,y,z``      any   Fire on any matching expression; can combine any number of any of the above expressions
============== ===== =======================================================================================


Daylight saving time behavior
-----------------------------

The cron trigger works with the so-called "wall clock" time. Thus, if the selected time zone observes DST (daylight
saving time), you should be aware that it may cause unexpected behavior with the cron trigger when entering or leaving
DST. When switching from standard time to daylight saving time, clocks are moved either one hour or half an hour
forward, depending on the time zone. Likewise, when switching back to standard time, clocks are moved one hour or half
an hour backward. This will cause some time periods to either not exist at all, or be repeated. If your schedule would
have the job executed on either one of these periods, it may execute more often or less often than expected.
This is not a bug. If you wish to avoid this, either use a timezone that does not observe DST, for instance UTC.
Alternatively, just find out about the DST switch times and avoid them in your scheduling.

For example, the following schedule may be problematic::

    # In the Europe/Helsinki timezone, this will not execute at all on the last sunday morning of March
    # Likewise, it will execute twice on the last sunday morning of October
    sched.add_job(job_function, 'cron', hour=3, minute=30)


Examples
--------

::

    from apscheduler.schedulers.blocking import BlockingScheduler


    def job_function():
        print "Hello World"

    sched = BlockingScheduler()

    # Schedules job_function to be run on the third Friday
    # of June, July, August, November and December at 00:00, 01:00, 02:00 and 03:00
    sched.add_job(job_function, 'cron', month='6-8,11-12', day='3rd fri', hour='0-3')

    sched.start()


You can use ``start_date`` and ``end_date`` to limit the total time in which the schedule runs::

    # Runs from Monday to Friday at 5:30 (am) until 2014-05-30 00:00:00
    sched.add_job(job_function, 'cron', day_of_week='mon-fri', hour=5, minute=30, end_date='2014-05-30')


The :meth:`~apscheduler.schedulers.base.BaseScheduler.scheduled_job` decorator works nicely too::

    @sched.scheduled_job('cron', id='my_job_id', day='last sun')
    def some_decorated_task():
        print("I am printed at 00:00:00 on the last Sunday of every month!")
