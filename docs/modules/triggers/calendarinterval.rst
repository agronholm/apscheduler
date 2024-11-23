:mod:`apscheduler.triggers.calendarinterval`
============================================

.. automodule:: apscheduler.triggers.calendarinterval

API
---

Trigger alias for :meth:`~apscheduler.schedulers.base.BaseScheduler.add_job`:
``calendarinterval``

.. autoclass:: CalendarIntervalTrigger
    :show-inheritance:

Examples
--------

::

    from datetime import datetime

    from apscheduler.schedulers.blocking import BlockingScheduler


    def job_function():
        print("Hello World")

    sched = BlockingScheduler()

    # Schedule job_function to be called every two months at 16:49:00
    sched.add_job(job_function, 'calendarinterval', months=2, hour=16, minute=49)

    sched.start()


You can use ``start_date`` and ``end_date`` to limit the total time in which the
schedule runs::

    # The same as before, but starts on 2010-10-10 and stops on 2014-06-15
    sched.add_job(
        job_function,
        'calendarinterval',
        months=2,
        hour=16,
        minute=49,
        start_date='2010-10-10',
        end_date='2014-06-15'
    )


The ``jitter`` option enables you to add a random component to the execution time.
This might be useful if you have multiple servers and don't want them to run a job at
the exact same moment or if you want to prevent multiple jobs with similar options from
always running concurrently::

    # Run `job_function` every two months with an extra delay picked randomly between 0
    # and 120 seconds
    sched.add_job(job_function, 'calendarinterval', months=2, jitter=120)
