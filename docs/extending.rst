#####################
Extending APScheduler
#####################

This document is meant to explain how to develop your custom triggers and data stores.


Custom triggers
---------------

.. py:currentmodule:: apscheduler.triggers

The built-in triggers cover the needs of the majority of all users, particularly so when
combined using :class:`~.combining.AndTrigger` and :class:`~.combining.OrTrigger`.
However, some users may need specialized scheduling logic. This can be accomplished by
creating your own custom trigger class.

To implement your scheduling logic, create a new class that inherits from the
:class:`~..abc.Trigger` interface class::

    from __future__ import annotations

    from apscheduler.abc import Trigger

    class MyCustomTrigger(Trigger):
        def next() -> datetime | None:
            ... # Your custom logic here

        def __getstate__():
            ... # Return the serializable state here

        def __setstate__(state):
            ... # Restore the state from the return value of __getstate__()

Requirements and constraints for trigger classes:

* :meth:`~..abc.Trigger.next` must always either return a timezone aware
  :class:`datetime` object or :data:`None` if a new run time cannot be calculated
* :meth:`~..abc.Trigger.next` must never return the same :class:`~datetime.datetime`
  twice and never one that is earlier than the previously returned one
* :meth:`~..abc.Trigger.__setstate__` must accept the return value of
  :meth:`~..abc.Trigger.__getstate__` and restore the trigger to the functionally same
  state as the original

Triggers are stateful objects. The :meth:`~..abc.Trigger.next` method is where you
determine the next run time based on the current state of the trigger. The trigger's
internal state needs to be updated before returning to ensure that the trigger won't
return the same datetime on the next call. The trigger code does **not** need to be
thread-safe.


Custom data stores
------------------

If you want to make use of some external service to store the scheduler data, and it's
not covered by a built-in data store implementation, you may want to create a custom
data store class. It should be noted that custom data stores are substantially harder to
implement than custom triggers.

Data store classes have the following design requirements:

* Must publish the appropriate events to an event broker
* Code must be thread safe (synchronous API) or task safe (asynchronous API)

The data store class needs to inherit from either :class:`~..abc.DataStore` or
:class:`~..abc.AsyncDataStore`, depending on whether you want to implement the store
using synchronous or asynchronous APIs:

.. tabs::

   .. code-tab:: python Synchronous

        from apscheduler.abc import DataStore, EventBroker

        class MyCustomDataStore(Datastore):
            def start(self, event_broker: EventBroker) -> None:
                ...  # Save the event broker in a member attribute and initialize the store

            def stop(self, *, force: bool = False) -> None:
                ...  # Shut down the store

            # See the interface class for the rest of the abstract methods

   .. code-tab:: python Asynchronous

        from apscheduler.abc import AsyncDataStore, AsyncEventBroker

        class MyCustomDataStore(AsyncDatastore):
            async def start(self, event_broker: AsyncEventBroker) -> None:
                ...  # Save the event broker in a member attribute and initialize the store

            async def stop(self, *, force: bool = False) -> None:
                ...  # Shut down the store

            # See the interface class for the rest of the abstract methods

Handling temporary failures
+++++++++++++++++++++++++++

If you plan to make the data store implementation public, it is strongly recommended
that you make an effort to ensure that the implementation can tolerate the loss of
connectivity to the backing store. The Tenacity_ library is used for this purpose by the
built-in stores to retry operations in case of a disconnection. If you use it to retry
operations when exceptions are raised, it is important to only do that in cases of
*temporary* errors, like connectivity loss, and not in cases like authentication
failure, missing database and so forth. See the built-in data store implementations and
Tenacity_ documentation for more information on how to pick the exceptions on which to
retry the operations.

.. _Tenacity: https://pypi.org/project/tenacity/
