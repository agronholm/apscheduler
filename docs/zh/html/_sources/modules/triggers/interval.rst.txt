:mod:`apscheduler.triggers.interval`
====================================

.. automodule:: apscheduler.triggers.interval

API
---

Trigger alias for :meth:`~apscheduler.schedulers.base.BaseScheduler.add_job`: ``interval``

.. autoclass:: IntervalTrigger
    :show-inheritance:


Introduction
------------

This method schedules jobs to be run periodically, on selected intervals.

You can also specify the starting date and ending dates for the schedule through the ``start_date`` and ``end_date``
parameters, respectively. They can be given as a date/datetime object or text (in the
`ISO 8601 <https://en.wikipedia.org/wiki/ISO_8601>`_ format).

If the start date is in the past, the trigger will not fire many times retroactively but instead calculates the next
run time from the current time, based on the past start time.


Examples
--------

::

    from datetime import datetime

    from apscheduler.schedulers.blocking import BlockingScheduler


    def job_function():
        print("Hello World")

    sched = BlockingScheduler()

    # Schedule job_function to be called every two hours
    sched.add_job(job_function, 'interval', hours=2)

    sched.start()


You can use ``start_date`` and ``end_date`` to limit the total time in which the schedule runs::

    # The same as before, but starts on 2010-10-10 at 9:30 and stops on 2014-06-15 at 11:00
    sched.add_job(job_function, 'interval', hours=2, start_date='2010-10-10 09:30:00', end_date='2014-06-15 11:00:00)


The :meth:`~apscheduler.schedulers.base.BaseScheduler.scheduled_job` decorator works nicely too::

    from apscheduler.scheduler import BlockingScheduler

    @sched.scheduled_job('interval', id='my_job_id', hours=2)
    def job_function():
        print("Hello World")
