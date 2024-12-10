#!/usr/bin/env python

from setuptools import setup

setup(name='tap-sendgrid',
      version='1.0.4',
      description='Singer.io tap for extracting data from the SendGrid API',
      author='Stitch',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_sendgrid'],
      install_requires=['singer-python==5.0.4',
                        'requests==2.20.0',
                        'pendulum==1.2.0',
                        'pytz==2024.2',
                        ],
      entry_points='''
          [console_scripts]
          tap-sendgrid=tap_sendgrid:main
      ''',
      packages=['tap_sendgrid'],
      package_data={
          'tap_sendgrid/schemas': [
                "contacts.json",
                "global_suppressions.json",
                "groups_members.json",
                "groups_all.json",
                "invalids.json",
                "lists_all.json",
                "lists_members.json",
                "segments_all.json",
                "segments_members.json",
                "templates_all.json",
                "blocks.json",
                "bounces.json",
                "campaigns.json",
                "spam_reports.json",
              ]
         },
      include_package_data=True
)
