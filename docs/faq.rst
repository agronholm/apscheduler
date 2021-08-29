##########################
Frequently Asked Questions
##########################

Why doesn't the scheduler run my jobs?
======================================

This could be caused by a number of things. The two most common issues are:

#. Running the scheduler inside a uWSGI worker process while threads have not been enabled (see the
   next section for this)
#. Running a :class:`~apscheduler.schedulers.background.BackgroundScheduler` and then letting the
   execution reach the end of the script

To demonstrate the latter case, a script like this will **not work**::

    from apscheduler.schedulers.background import BackgroundScheduler

    def myjob():
        print('hello')

    scheduler = BackgroundScheduler()
    scheduler.start()
    scheduler.add_job(myjob, 'cron', hour=0)

The script above will **exit** right after calling ``add_job()`` so the scheduler will not have a
chance to run the scheduled job.

If you're having any other issue, then enabling debug logging as instructed in the
:ref:`troubleshooting` section should shed some light into the problem.

Why am I getting a ValueError?
==============================

If you're receiving an error like the following::

   ValueError: This Job cannot be serialized since the reference to its callable (<bound method xxxxxxxx.on_crn_field_submission
   of <__main__.xxxxxxx object at xxxxxxxxxxxxx>>) could not be determined. Consider giving a textual reference (module:function
   name) instead.

This means that the function you are attempting to schedule has one of the following problems:

* It is a lambda function (e.g. ``lambda x: x + 1``)
* It is a bound method (function tied to a particular instance of some class)
* It is a nested function (function inside another function)
* You are trying to schedule a function that is not tied to any actual module (such as a function
  defined in the REPL, hence ``__main__`` as the module name)

In these cases, it is impossible for the scheduler to determine a "lookup path" to find that
specific function instance in situations where, for example, the scheduler process is restarted,
or a process pool worker is being sent the related job object.

Common workarounds for these problems include:

* Converting a lambda to a regular function
* Moving a nested function to the module level or to class level as either a class method or a
  static method
* In case of a bound method, passing the unbound version (``YourClass.method_name``) as the target
  function to ``add_job()`` with the class instance as the first argument (so it gets passed as the
  ``self`` argument)

How can I use APScheduler with uWSGI?
=====================================

uWSGI employs some tricks which disable the Global Interpreter Lock and with it, the use of threads
which are vital to the operation of APScheduler. To fix this, you need to re-enable the GIL using
the ``--enable-threads`` switch. See the `uWSGI documentation <uWSGI-threads>`_ for more details.

Also, assuming that you will run more than one worker process (as you typically would in
production), you should also read the next section.

.. _uWSGI-threads: https://uwsgi-docs.readthedocs.io/en/latest/WSGIquickstart.html#a-note-on-python-threads

How do I share a single job store among one or more worker processes?
=====================================================================

Short answer: You can't.

Long answer: Sharing a persistent job store among two or more processes will lead to incorrect
scheduler behavior like duplicate execution or the scheduler missing jobs, etc. This is because
APScheduler does not currently have any interprocess synchronization and signalling scheme that
would enable the scheduler to be notified when a job has been added, modified or removed from a job
store.

Workaround: Run the scheduler in a dedicated process and connect to it via some sort of remote
access mechanism like RPyC_, gRPC_ or an HTTP server. The source repository contains an example_ of
a RPyC based service that is accessed by a client.

.. _RPyC: https://rpyc.readthedocs.io/en/latest/
.. _gRPC: https://www.google.com/url?sa=t&rct=j&q=&esrc=s&source=web&cd=1&cad=rja&uact=8&ved=2ahUKEwj-wMe-1eLcAhXSbZoKHdzGDZsQFjAAegQICRAB&url=https%3A%2F%2Fgrpc.io%2F&usg=AOvVaw0Jt5Y0OKbHd8MdFt9Kc2FO
.. _example: https://github.com/agronholm/apscheduler/tree/3.x/examples/rpc

How do I use APScheduler in a web application?
==============================================

First read through the previous section.

If you're running Django, you may want to check out django_apscheduler_. Note, however, that this
is a third party library and APScheduler developers are not responsible for it.

Likewise, there is an unofficial extension called Flask-APScheduler_ which may or may not be useful
when running APScheduler with Flask.

For Pyramid users, the pyramid_scheduler_ library may potentially be helpful.

Other than that, you pretty much run APScheduler normally, usually using
:class:`~apscheduler.schedulers.background.BackgroundScheduler`. If you're running an asynchronous
web framework like aiohttp_, you probably want to use a different scheduler in order to take some
advantage of the asynchronous nature of the framework.

Is there a graphical user interface for APScheduler?
====================================================

No graphical interface is provided by the library itself. However, there are some third party
implementations, but APScheduler developers are not responsible for them. Here is a potentially
incomplete list:

* django_apscheduler_
* apschedulerweb_
* `Nextdoor scheduler`_

.. _django_apscheduler: https://pypi.org/project/django-apscheduler/
.. _Flask-APScheduler: https://pypi.org/project/flask-apscheduler/
.. _pyramid_scheduler: https://github.com/cadithealth/pyramid_scheduler
.. _aiohttp: https://pypi.org/project/aiohttp/
.. _apschedulerweb: https://github.com/marwinxxii/apschedulerweb
.. _Nextdoor scheduler: https://github.com/Nextdoor/ndscheduler
