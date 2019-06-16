:mod:`apscheduler.triggers.calendarinterval`
============================================

.. automodule:: apscheduler.triggers.calendarinterval

API
---

Trigger alias for :meth:`~apscheduler.schedulers.base.BaseScheduler.add_job`: ``calendarinterval``

.. autoclass:: CalendarIntervalTrigger
    :show-inheritance:


Introduction
------------

This method schedules jobs to be run on calendar-based intervals, always at the same wall-clock
time. You can specify years, months, weeks and days as the interval, and they will be added to the
previous fire time in that order when calculating the next fire time.

You can also specify the starting date and ending dates for the schedule through the ``start_date``
and ``end_date`` parameters, respectively. They can be given as a date/datetime object or text (in
the `ISO 8601 <https://en.wikipedia.org/wiki/ISO_8601>`_ format).

If the start date is in the past, the trigger will not fire many times retroactively but instead
calculates the next run time from the current time, based on the past start time.


Examples
--------

::

    from datetime import datetime

    from apscheduler.schedulers.blocking import BlockingScheduler


    def job_function():
        print("Hello World")

    sched = BlockingScheduler()

    # Schedule job_function to be called every month at 15:36:00, starting from today
    sched.add_job(job_function, 'calendarinterval', months=1, hour=15, minute=36)

    sched.start()


You can use ``start_date`` and ``end_date`` to limit the total time in which the schedule runs::

    # The same as before, but starts on 2019-06-16 and stops on 2020-03-16
    sched.add_job(job_function, 'calendarinterval', months=2, start_date='2019-06-16',
                  end_date='2020-03-16', hour=15, minute=36)


The :meth:`~apscheduler.schedulers.base.BaseScheduler.scheduled_job` decorator works nicely too::

    from apscheduler.scheduler import BlockingScheduler

    @sched.scheduled_job('calendarinterval', id='my_job_id', weeks=2)
    def job_function():
        print("Hello World")
