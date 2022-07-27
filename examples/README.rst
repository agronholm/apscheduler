APScheduler practical examples
==============================

.. highlight:: bash

This directory contains a number of practical examples for running APScheduler in a
variety of configurations.

Prerequisites
-------------

Most examples use one or more external services for data sharing and synchronization.
To start these services, you need Docker_ installed. Each example lists the services
it needs (if any) in the module file, so you can start these services selectively.

On Linux, if you're using the vendor provided system package for Docker instead of
Docker Desktop, you may need to install the compose (v2) plugin (named
``docker-compose-plugin``, or similar) separately.

.. note:: If you're still using the Python-based docker-compose tool (aka compose v1),
          replace ``docker compose`` with ``docker-compose``.

To start all the services, run this command anywhere within the project directory::

    docker compose up -d

To start just a specific service, you can pass its name as an argument::

    docker compose up -d postgresql

To shut down the services and delete all their data::

    docker compose down -v

In addition to having these background services running, you may need to install
specific extra dependencies, like database drivers. Each example module has its required
dependencies listed in the module comment at the top.

.. _Docker: https://docs.docker.com/desktop/#download-and-install

Standalone examples
-------------------

The examples in the ``standalone`` directory demonstrate how to run the scheduler in the
foreground, without anything else going on in the same process.

The directory contains four modules:

- ``async_memory.py``: Basic asynchronous scheduler using the default memory-based data
  store
- ``async_postgres.py``: Basic asynchronous scheduler using the asynchronous SQLAlchemy
  data store with a PostgreSQL back-end
- ``async_mysql.py``: Basic asynchronous scheduler using the asynchronous SQLAlchemy
  data store with a MySQL back-end
- ``sync_mysql.py``: Basic synchronous scheduler using the default memory-based data
  store

Schedulers in web apps
----------------------

The examples in the ``web`` directory demonstrate how to run the scheduler inside a web
application (ASGI_ or WSGI_).

The directory contains five modules:

- ``asgi_noframework.py``: Trivial ASGI_ application, with middleware that starts and
  stops the scheduler as part of the ASGI lifecycle
- ``asgi_fastapi.py``: Trivial FastAPI_ application, with middleware that starts and
  stops the scheduler as part of the ASGI_ lifecycle
- ``asgi_starlette.py``: Trivial Starlette_ application, with middleware that starts and
  stops the scheduler as part of the ASGI_ lifecycle
- ``wsgi_noframework.py``: Trivial WSGI_ application where the scheduler is started in a
  background thread
- ``wsgi_flask.py``: Trivial Flask_ application where the scheduler is started in a
  background thread

.. note:: There is no Django example available yet.

To run any of the ASGI_ examples::

    uvicorn <filename_without_py_extension>:app

To run any of the WSGI_ examples::

    uwsgi -T --http :8000 --wsgi-file <filename>

.. _ASGI: https://asgi.readthedocs.io/en/latest/introduction.html
.. _WSGI: https://wsgi.readthedocs.io/en/latest/what.html
.. _FastAPI: https://fastapi.tiangolo.com/
.. _Starlette: https://www.starlette.io/
.. _Flask: https://flask.palletsprojects.com/

Separate scheduler and worker
-----------------------------

The example in the ``separate_worker`` directory demonstrates the ability to run
schedulers and workers separately. The directory contains three modules:

- ``sync_scheduler.py``: Runs a scheduler (without an internal worker) and adds/updates
  a schedule
- ``sync_worker.py``: Runs a worker only
- ``tasks.py``: Contains the task code (don't try to run this directly; it does nothing)

The reason for the task function being in a separate module is because when you run
either the ``sync_scheduler`` or ``sync_worker`` script, that script is imported as the
``__main__`` module, so if the scheduler schedules ``__main__:tick`` as the task, then
the worker would not be able to find it because its own script would also be named
``__main__``.

To run the example, you need to have both the worker and scheduler scripts running at
the same time. To run the worker::

    python sync_worker.py

To run the scheduler::

    python sync_scheduler.py

You can run multiple schedulers and workers at the same time within this example. If you
run multiple workers, the message might be printed on the console of a different worker
each time the job is run. Running multiple schedulers should have no visible effect, and
as long as at least one scheduler is running, the scheduled task should keep running
periodically on one of the workers.
