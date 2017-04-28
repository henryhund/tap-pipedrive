#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-pipedrive',
      version='0.1.0',
      description='Singer.io tap for extracting data from the Pipedrive API',
      author='Henry Hund (henryhund@gmail.com)',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_pipedrive'],
      install_requires=['singer-python==1.6.0a2',
                        'requests==2.13.0'],
      entry_points='''
          [console_scripts]
          tap-pipedrive=tap_pipedrive:main
      ''',
      packages=['tap_pipedrive'],
      package_data = {
          'tap_pipedrive': [
              # 'commits.json',
              # 'issues.json'
              ]
          }
)
