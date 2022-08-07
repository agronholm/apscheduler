API reference
=============

Data structures
---------------

.. autoclass:: apscheduler.Task
.. autoclass:: apscheduler.Schedule
.. autoclass:: apscheduler.Job
.. autoclass:: apscheduler.JobInfo
.. autoclass:: apscheduler.JobResult
.. autoclass:: apscheduler.RetrySettings

Schedulers
----------

.. autoclass:: apscheduler.schedulers.sync.Scheduler
.. autoclass:: apscheduler.schedulers.async_.AsyncScheduler

Workers
-------

.. autoclass:: apscheduler.workers.sync.Worker
.. autoclass:: apscheduler.workers.async_.AsyncWorker

Data stores
-----------

.. autoclass:: apscheduler.datastores.memory.MemoryDataStore
.. autoclass:: apscheduler.datastores.sqlalchemy.SQLAlchemyDataStore
.. autoclass:: apscheduler.datastores.async_sqlalchemy.AsyncSQLAlchemyDataStore
.. autoclass:: apscheduler.datastores.mongodb.MongoDBDataStore

Event brokers
-------------

.. autoclass:: apscheduler.eventbrokers.local.LocalEventBroker
.. autoclass:: apscheduler.eventbrokers.async_local.LocalAsyncEventBroker
.. autoclass:: apscheduler.eventbrokers.asyncpg.AsyncpgEventBroker
.. autoclass:: apscheduler.eventbrokers.mqtt.MQTTEventBroker
.. autoclass:: apscheduler.eventbrokers.redis.RedisEventBroker

Serializers
-----------

.. autoclass:: apscheduler.serializers.cbor.CBORSerializer
.. autoclass:: apscheduler.serializers.json.JSONSerializer
.. autoclass:: apscheduler.serializers.pickle.PickleSerializer

Triggers
--------

.. autoclass:: apscheduler.triggers.date.DateTrigger
.. autoclass:: apscheduler.triggers.interval.IntervalTrigger
.. autoclass:: apscheduler.triggers.calendarinterval.CalendarIntervalTrigger
.. autoclass:: apscheduler.triggers.combining.AndTrigger
.. autoclass:: apscheduler.triggers.combining.OrTrigger
.. autoclass:: apscheduler.triggers.cron.CronTrigger

Events
------

.. autoclass:: apscheduler.Event
.. autoclass:: apscheduler.DataStoreEvent
.. autoclass:: apscheduler.TaskAdded
.. autoclass:: apscheduler.TaskUpdated
.. autoclass:: apscheduler.TaskRemoved
.. autoclass:: apscheduler.ScheduleAdded
.. autoclass:: apscheduler.ScheduleUpdated
.. autoclass:: apscheduler.ScheduleRemoved
.. autoclass:: apscheduler.JobAdded
.. autoclass:: apscheduler.JobRemoved
.. autoclass:: apscheduler.ScheduleDeserializationFailed
.. autoclass:: apscheduler.JobDeserializationFailed
.. autoclass:: apscheduler.SchedulerEvent
.. autoclass:: apscheduler.SchedulerStarted
.. autoclass:: apscheduler.SchedulerStopped
.. autoclass:: apscheduler.WorkerEvent
.. autoclass:: apscheduler.WorkerStarted
.. autoclass:: apscheduler.WorkerStopped
.. autoclass:: apscheduler.JobAcquired
.. autoclass:: apscheduler.JobReleased

Enumerated types
----------------

.. autoclass:: apscheduler.RunState
.. autoclass:: apscheduler.JobOutcome
.. autoclass:: apscheduler.ConflictPolicy
.. autoclass:: apscheduler.CoalescePolicy

Context variables
-----------------

.. autodata:: apscheduler.current_scheduler
.. autodata:: apscheduler.current_worker
.. autodata:: apscheduler.current_job

Exceptions
----------

.. autoexception:: apscheduler.TaskLookupError
.. autoexception:: apscheduler.ScheduleLookupError
.. autoexception:: apscheduler.JobLookupError
.. autoexception:: apscheduler.JobResultNotReady
.. autoexception:: apscheduler.JobCancelled
.. autoexception:: apscheduler.JobDeadlineMissed
.. autoexception:: apscheduler.ConflictingIdError
.. autoexception:: apscheduler.SerializationError
.. autoexception:: apscheduler.DeserializationError
.. autoexception:: apscheduler.MaxIterationsReached
