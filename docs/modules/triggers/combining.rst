:mod:`apscheduler.triggers.combining`
=====================================

These triggers combine the behavior of other triggers in different ways to produce schedules more
complex than would be possible with any single built-in trigger.

.. automodule:: apscheduler.triggers.combining

API
---

.. autoclass:: AndTrigger

.. autoclass:: OrTrigger


Examples
--------

Run ``job_function`` every 2 hours, but only on Saturdays and Sundays::

    from apscheduler.triggers.combining import AndTrigger
    from apscheduler.triggers.interval import IntervalTrigger
    from apscheduler.triggers.cron import CronTrigger


    trigger = AndTrigger([IntervalTrigger(hours=2),
                          CronTrigger(day_of_week='sat,sun')])
    scheduler.add_job(job_function, trigger)

Run ``job_function`` every Monday at 2am and every Tuesday at 3pm::

    trigger = OrTrigger([CronTrigger(day_of_week='mon', hour=2),
                         CronTrigger(day_of_week='tue', hour=15)])
    scheduler.add_job(job_function, trigger)
