:mod:`apscheduler.schedulers.base`
==================================

.. automodule:: apscheduler.schedulers.base

API
---

.. autoclass:: BaseScheduler
    :members:
    :exclude-members: add_job
    :member-order: alphabetical

    .. automethod:: add_job(func, trigger=None, args=None, kwargs=None, id=None, name=None, misfire_grace_time=undefined, coalesce=undefined, max_runs=undefined, max_instances=undefined, jobstore='default', executor='default', replace_existing=False, **trigger_args)

.. autoexception:: SchedulerAlreadyRunningError
