# coding: utf-8
from setuptools import setup, find_packages

setup(
    name='APScheduler',
    version='1.0',
    description='Advanced Python Scheduler',
    author='Alex Grönholm',
    author_email='apscheduler@nextday.fi',
    #url='',
    package_dir = {'': 'src'},
    packages=find_packages('src'),
    include_package_data=False,
    test_suite='nose.collector',
)
