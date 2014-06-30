:mod:`apscheduler.events`
============================

.. automodule:: apscheduler.events

API
---

.. autoclass:: SchedulerEvent
    :members:

.. autoclass:: JobEvent
    :members:
    :show-inheritance:

.. autoclass:: JobExecutionEvent
    :members:
    :show-inheritance:


Event codes
-----------

The following event codes are numeric constants importable from :mod:`apscheduler.events`.

.. list-table::
  :header-rows: 1

  * - Constant
    - Description
    - Event class
  * - EVENT_SCHEDULER_START
    - The scheduler was started
    - :class:`SchedulerEvent`
  * - EVENT_SCHEDULER_SHUTDOWN
    - The scheduler was shut down
    - :class:`SchedulerEvent`
  * - EVENT_EXECUTOR_ADDED
    - An executor was added to the scheduler
    - :class:`SchedulerEvent`
  * - EVENT_EXECUTOR_REMOVED
    - An executor was added to the scheduler
    - :class:`SchedulerEvent`
  * - EVENT_JOBSTORE_ADDED
    - A job store was added to the scheduler
    - :class:`SchedulerEvent`
  * - EVENT_JOBSTORE_REMOVED
    - A job store was removed from the scheduler
    - :class:`SchedulerEvent`
  * - EVENT_ALL_JOBS_REMOVED
    - All jobs were removed from either all job stores or one particular job store
    - :class:`SchedulerEvent`
  * - EVENT_JOB_ADDED
    - A job was added to a job store
    - :class:`JobEvent`
  * - EVENT_JOB_REMOVED
    - A job was removed from a job store
    - :class:`JobEvent`
  * - EVENT_JOB_MODIFIED
    - A job was modified from outside the scheduler
    - :class:`JobEvent`
  * - EVENT_JOB_EXECUTED
    - A job was executed successfully
    - :class:`JobExecutionEvent`
  * - EVENT_JOB_ERROR
    - A job raised an exception during execution
    - :class:`JobExecutionEvent`
  * - EVENT_JOB_MISSED
    - A job's execution was missed
    - :class:`JobExecutionEvent`
  * - EVENT_ALL
    - A catch-all mask that includes every event type
    - N/A
