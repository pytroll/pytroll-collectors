#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2018 PyTroll team

# Author(s):

#   panu.lahtinen@fmi.fi

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

"""Unit testing for segment gatherer
"""

import unittest
import datetime as dt
import os
import os.path

from pytroll_collectors.segments import SegmentGatherer, ini_to_dict
from pytroll_collectors.helper_functions import read_yaml

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_SINGLE = read_yaml(os.path.join(THIS_DIR, "data/segments_single.yaml"))
# CONFIG_DOUBLE = read_yaml(os.path.join(THIS_DIR, "data/segments_double.yaml"))
# CONFIG_INI = ini_to_dict(os.path.join(THIS_DIR, "data/segments.ini", "msg"))

class TestSegmentGatherer(unittest.TestCase):

    def setUp(self):
        """Setting up the testing
        """
        self.gyml1 = SegmentGatherer(CONFIG_SINGLE)
        # self.gyml2 = SegmentGatherer(CONFIG_DOUBLE)
        # self.gini = SegmentGatherer(CONFIG_INI)

    def test_init(self):
        self.assertTrue(self.gyml1._config == CONFIG_SINGLE)
        self.assertTrue(self.gyml1._subject is None)
        self.assertEqual(self.gyml1._patterns.keys(), ['msg'])
        self.assertEqual(self.gyml1._parsers.keys(), ['msg'])
        self.assertEqual(len(self.gyml1.slots.keys()), 0)
        self.assertEqual(self.gyml1.time_name, 'start_time')
        self.assertFalse(self.gyml1._loop)
        self.assertEqual(self.gyml1._time_tolerance, 30)
        self.assertEqual(self.gyml1._timeliness.total_seconds(), 10)
        self.assertEqual(self.gyml1._listener, None)
        self.assertEqual(self.gyml1._publisher, None)


    def tearDown(self):
        """Closing down
        """
        pass


def suite():
    """The suite for test_trollduction
    """
    loader = unittest.TestLoader()
    mysuite = unittest.TestSuite()
    mysuite.addTest(loader.loadTestsFromTestCase(TestSegmentGatherer))

    return mysuite

if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=2).run(suite())
