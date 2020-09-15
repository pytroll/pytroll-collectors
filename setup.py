#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2013 - 2019 PyTroll Community

# Author(s):

#   Martin Raspaud <martin.raspaud@smhi.se>
#   Adam Dybbroe <adam.dybbroe@smhi.se>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Setup for pytroll_collectors."""
from setuptools import setup
import versioneer
import os

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
# Set PPP_CONFIG_DIR for tests
os.environ['PPP_CONFIG_DIR'] = os.path.join(THIS_DIR, 'pytroll_collectors', 'tests', 'data')


setup(name="pytroll_collectors",
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      description='Pytroll data collectors',
      author='Martin Raspaud',
      author_email='martin.raspaud@smhi.se',
      classifiers=["Development Status :: 3 - Alpha",
                   "Intended Audience :: Science/Research",
                   "License :: OSI Approved :: GNU General Public License v3 " +
                   "or later (GPLv3+)",
                   "Operating System :: OS Independent",
                   "Programming Language :: Python",
                   "Topic :: Scientific/Engineering"],
      url="https://github.com/pytroll/pytroll-collectors",
      packages=['pytroll_collectors',
                'pytroll_collectors.tests', ],
      scripts=['bin/trollstalker.py',
               'bin/trollstalker2.py',
               'bin/gatherer.py',
               'bin/segment_gatherer.py',
               'bin/cat.py',
               'bin/catter.py',
               'bin/scisys_receiver.py',
               'bin/zipcollector_runner.py'
               ],
      data_files=[],
      zip_safe=False,
      install_requires=['pykdtree', 'pyinotify', 'posttroll>=1.3.0',
                        'trollsift', 'netifaces', 'watchdog',
                        'pytroll-schedule', 'pyresample',
                        'pillow', 'pycoast', 'six'],
      tests_require=['mock', 'scipy', 'trollsift', 'pillow', 'six'],
      )
