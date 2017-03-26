###########################
Contributing to APScheduler
###########################

If you wish to add a feature or fix a bug in APScheduler, you need to follow certain procedures and rules to get your
changes accepted. This is to maintain the high quality of the code base.


Contribution Process
====================

1. Fork the project on Github
2. Clone the fork to your local machine
3. Make the changes to the project
4. Run the test suite with tox (if you changed any code)
5. Repeat steps 3-4 until the test suite passes
6. Commit if you haven't already
7. Push the changes to your Github fork
8. Make a pull request on Github

There is no need to update the change log -- this will be done prior to the next release at the latest.
Should the test suite fail even before your changes (which should be rare), make sure you're at least not adding to the
failures.


Development Dependencies
========================

To fully run the test suite, you will need at least:

 * A MongoDB server
 * A Redis server
 * A Zookeeper server

For other dependencies, it's best to look in tox.ini and install what is appropriate for the Python version you're
using.


Code Style
==========

This project uses PEP 8 rules with a maximum column limit of 120 characters instead of the standard 79.
This limit applies to all text files (source code, tests, documentation).
In particular, remember to group the imports correctly (standard library imports first, third party libs second,
project libraries third, conditional imports last). The PEP 8 checker does not check for this.
If in doubt, just follow the surrounding code style as closely as possible.


Testing
=======

Running the test suite is done using the tox utility. This will test the code base against all supported Python
versions and checks for PEP 8 violations as well.

Since running the tests on every supported Python version can take quite a long time, it is recommended that during the
development cycle py.test is used directly. Before finishing, tox should however be used to make sure the code works on
all supported Python versions.

Any nontrivial code changes must be accompanied with the appropriate tests.
The tests should not only maintain the coverage, but should test any new functionality or bug fixes reasonably well.
If you're fixing a bug, first make sure you have a test which fails against the unpatched codebase and succeeds against
the fixed version. Naturally, the test suite has to pass on every Python version. If setting up all the required Python
interpreters seems like too much trouble, make sure that it at least passes on the lowest supported versions of both
Python 2 and 3.
