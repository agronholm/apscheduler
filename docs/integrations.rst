Integrating with application frameworks
=======================================

WSGI
----

To integrate APScheduler with web frameworks using WSGI_ (Web Server Gateway Interface),
you need to use the synchronous scheduler and start it as a side effect of importing the
module that contains your application instance::

    from apscheduler.schedulers.sync import Scheduler


    def app(environ, start_response):
        """Trivial example of a WSGI application."""
        response_body = b"Hello, World!"
        response_headers = [
            ("Content-Type", "text/plain"),
            ("Content-Length", str(len(response_body))),
        ]
        start_response(200, response_headers)
        return [response_body]


    scheduler = Scheduler()
    scheduler.start_in_background()

Assuming you saved this as ``example.py``, you can now start the application with uWSGI_
with:

.. code-block:: bash

    uwsgi --enable-threads --http :8080 --wsgi-file example.py

The ``--enable-threads`` (or ``-T``) option is necessary because uWSGI disables threads
by default which then prevents the scheduler from working. See the
`uWSGI documentation <uWSGI-threads>`_ for more details.

.. note::
    The :meth:`.schedulers.sync.Scheduler.start_in_background` method installs an
    :mod:`atexit` hook that shuts down the scheduler gracefully when the worker process
    exits.

.. _WSGI: https://wsgi.readthedocs.io/en/latest/what.html
.. _uWSGI: https://www.fullstackpython.com/uwsgi.html
.. _uWSGI-threads: https://uwsgi-docs.readthedocs.io/en/latest/WSGIquickstart.html#a-note-on-python-threads

ASGI
----

To integrate APScheduler with web frameworks using ASGI_ (Asynchronous Server Gateway
Interface), you need to use the asynchronous scheduler and tie its lifespan to the
lifespan of the application by wrapping it in middleware, as follows::

    from apscheduler.schedulers.async_ import AsyncScheduler


    async def app(scope, receive, send):
        """Trivial example of an ASGI application."""
        if scope["type"] == "http":
            await receive()
            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [
                        [b"content-type", b"text/plain"],
                    ],
                }
            )
            await send(
                {
                    "type": "http.response.body",
                    "body": b"Hello, world!",
                    "more_body": False,
                }
            )
        elif scope["type"] == "lifespan":
            while True:
                message = await receive()
                if message["type"] == "lifespan.startup":
                    await send({"type": "lifespan.startup.complete"})
                elif message["type"] == "lifespan.shutdown":
                    await send({"type": "lifespan.shutdown.complete"})
                    return


    async def scheduler_middleware(scope, receive, send):
        if scope['type'] == 'lifespan':
            async with AsyncScheduler() as scheduler:
                await app(scope, receive, send)
        else:
            await app(scope, receive, send)

Assuming you saved this as ``example.py``, you can then run this with Hypercorn_:

.. code-block:: bash

    hypercorn example:scheduler_middleware

or with Uvicorn_:

.. code-block:: bash

    uvicorn example:scheduler_middleware

.. _ASGI: https://asgi.readthedocs.io/en/latest/index.html
.. _Hypercorn: https://gitlab.com/pgjones/hypercorn/
.. _Uvicorn: https://www.uvicorn.org/
