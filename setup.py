# coding: utf-8
try:
    from setuptools import setup

    extras = dict(zip_safe=False,
                  test_suite='nose.collector',
                  tests_require=['nose'])
except ImportError:
    from distutils.core import setup
    extras = {}

import apscheduler


setup(
    name='APScheduler',
    version=apscheduler.release,
    description='In-process task scheduler with Cron-like capabilities',
    long_description=open('README.rst').read(),
    author='Alex Gronholm',
    author_email='apscheduler@nextday.fi',
    url='http://pypi.python.org/pypi/APScheduler/',
    classifiers=[
      'Development Status :: 5 - Production/Stable',
      'Intended Audience :: Developers',
      'License :: OSI Approved :: MIT License',
      'Programming Language :: Python',
      'Programming Language :: Python :: 2.4',
      'Programming Language :: Python :: 2.5',
      'Programming Language :: Python :: 2.6',
      'Programming Language :: Python :: 2.7',
      'Programming Language :: Python :: 3',
      'Programming Language :: Python :: 3.1',
      'Programming Language :: Python :: 3.2',
    ],
    keywords='scheduling cron',
    license='MIT',
    packages=('apscheduler', 'apscheduler.jobstores', 'apscheduler.triggers',
              'apscheduler.triggers.cron'),
    **extras
)
