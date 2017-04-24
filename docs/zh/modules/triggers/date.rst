:mod:`apscheduler.triggers.date`
================================

.. automodule:: apscheduler.triggers.date

API
---

Trigger alias for :meth:`~apscheduler.schedulers.base.BaseScheduler.add_job`: ``date``

.. autoclass:: DateTrigger
    :show-inheritance:


Introduction
------------

This is the simplest possible method of scheduling a job. It schedules a job to be executed once at the specified time.
It is APScheduler's equivalent to the UNIX "at" command.

The ``run_date`` can be given either as a date/datetime object or text (in the
`ISO 8601 <https://en.wikipedia.org/wiki/ISO_8601>`_ format).


Examples
--------

::

    from datetime import date

    from apscheduler.schedulers.blocking import BlockingScheduler


    sched = BlockingScheduler()

    def my_job(text):
        print(text)

    # The job will be executed on November 6th, 2009
    sched.add_job(my_job, 'date', run_date=date(2009, 11, 6), args=['text'])

    sched.start()


You can specify the exact time when the job should be run::

    # The job will be executed on November 6th, 2009 at 16:30:05
    sched.add_job(my_job, 'date', run_date=datetime(2009, 11, 6, 16, 30, 5), args=['text'])


The run date can be given as text too::

    sched.add_job(my_job, 'date', run_date='2009-11-06 16:30:05', args=['text'])

To add a job to be run immediately::

    # The 'date' trigger and datetime.now() as run_date are implicit
    sched.add_job(my_job, args=['text'])
