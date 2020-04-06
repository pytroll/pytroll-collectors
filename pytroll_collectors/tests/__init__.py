#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014, 2015 Martin Raspaud

# Author(s):

#   Martin Raspaud <martin.raspaud@smhi.se>

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

"""The tests package."""

import unittest
# import doctest
from pytroll_collectors.tests import (test_helper_functions,
                                      test_scisys,
                                      test_trigger,
                                      test_segments)


def suite():
    """Test suite."""
    mysuite = unittest.TestSuite()
    # Test the documentation strings
    # mysuite.addTests(doctest.DocTestSuite(image))
    # Use the unittests also
    mysuite.addTests(test_helper_functions.suite())
    mysuite.addTests(test_scisys.suite())
    mysuite.addTests(test_trigger.suite())
    mysuite.addTests(test_segments.suite())

    return mysuite
