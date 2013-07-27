# coding: utf-8
import os.path

from setuptools import setup, find_packages

import apscheduler


here = os.path.dirname(__file__)
readme_path = os.path.join(here, 'README.rst')
readme = open(readme_path).read()

setup(
    name='APScheduler',
    version=apscheduler.release,
    description='In-process task scheduler with Cron-like capabilities',
    long_description=readme,
    author='Alex Gronholm',
    author_email='apscheduler@nextday.fi',
    url='http://pypi.python.org/pypi/APScheduler/',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3'
    ],
    keywords='scheduling cron',
    license='MIT',
    packages=find_packages(),
    install_requires=['six', 'python-dateutil'],
    test_suite='nose.collector',
    tests_require=['nose'],
    zip_safe=False,
    entry_points={
        'apscheduler.triggers': [
            'date = apscheduler.triggers.date:DateTrigger',
            'interval = apscheduler.triggers.interval:IntervalTrigger',
            'cron = apscheduler.triggers.cron:CronTrigger'
        ]
    }
)
