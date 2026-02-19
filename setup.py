#!/usr/bin/env python

from setuptools import setup

setup(name='tap-sendgrid',
      version='1.3.0',
      description='Singer.io tap for extracting data from the SendGrid API',
      author='Stitch',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_sendgrid'],
      install_requires=['singer-python==6.7.0',
                        'requests==2.32.5',
                        'pendulum==3.1.0',
                        'pytz==2025.2',
                        'backoff==2.2.1',
                        ],
      entry_points='''
          [console_scripts]
          tap-sendgrid=tap_sendgrid:main
      ''',
      packages=['tap_sendgrid'],
      package_data={
          'tap_sendgrid/schemas': [
                "global_suppressions.json",
                "groups_members.json",
                "groups_all.json",
                "invalids.json",
                "lists_all.json",
                "segments_all.json",
                "templates_all.json",
                "blocks.json",
                "bounces.json",
                "campaigns.json",
                "spam_reports.json",
              ]
         },
      include_package_data=True
)
