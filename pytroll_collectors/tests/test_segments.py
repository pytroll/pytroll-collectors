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

from pytroll_collectors.segments import SegmentGatherer, ini_to_dict
from helper_functions import read_yaml

CONFIG_SINGLE = read_yaml("data/segments_single.yaml")
# CONFIG_DOUBLE = read_yaml("data/segments_double.yaml")
# CONFIG_INI = ini_to_dict("data/segments.ini", "msg")

class TestSegmentGatherer(unittest.TestCase):

    def setUp(self):
        """Setting up the testing
        """
        gy1 = SegmentGatherer(CONFIG_SIGLE)
        # gy2 = SegmentGatherer(CONFIG_DOUBLE)
        # gini = SegmentGatherer(CONFIG_INI)

    def test_patterns(self):
        pass

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
