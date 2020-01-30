#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2015 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <a000680@c14526.ad.smhi.se>

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

"""Unit testing some general purpose helper functions."""

import unittest
from datetime import datetime

from pytroll_collectors.helper_functions import create_aligned_datetime_var


class TestTimeUtilities(unittest.TestCase):
    """Time utilities."""

    def test_create_aligned_datetime_var(self):
        """Test the create_aligned_datetime_var function."""
        # Run
        filepattern = "{start_time:%Y%m%d%H%M%S|align(5)}"
        result = create_aligned_datetime_var(filepattern,
                                             {'start_time':
                                              datetime(2015, 1, 9, 17, 10, 48)})
        # Assert
        self.assertEqual(result, datetime(2015, 1, 9, 17, 10, 0))

        # Run
        result = create_aligned_datetime_var(filepattern,
                                             {'start_time':
                                              datetime(2015, 1, 9, 17, 3, 0)})
        # Assert
        self.assertEqual(result, datetime(2015, 1, 9, 17, 0, 0))

        # Run
        result = create_aligned_datetime_var(filepattern,
                                             {'start_time':
                                              datetime(2015, 1, 9, 16, 59, 0)})
        # Assert
        self.assertEqual(result, datetime(2015, 1, 9, 16, 55, 0))

        # Run
        filepattern = "{start_time:%Y%m%d%H%M%S|align(15)}"

        result = create_aligned_datetime_var(filepattern,
                                             {'start_time':
                                              datetime(2015, 1, 9, 16, 59, 0)})
        # Assert
        self.assertEqual(result, datetime(2015, 1, 9, 16, 45, 0))

    def test_create_aligned_datetime_var_offsets(self):
        """Test the create_aligned_datetime_var function."""
        # Run
        filepattern = "{start_time:%Y%m%d%H%M%S|align(15,-2)}"
        result = create_aligned_datetime_var(filepattern,
                                             {'start_time':
                                              datetime(2015, 1, 9, 16, 59, 0)})
        # Assert
        self.assertEqual(result, datetime(2015, 1, 9, 17, 0, 0))

        filepattern = "{start_time:%Y%m%d%H%M%S|align(15,2)}"
        result = create_aligned_datetime_var(filepattern,
                                             {'start_time':
                                              datetime(2015, 1, 9, 17, 16, 0)})
        # Assert
        self.assertEqual(result, datetime(2015, 1, 9, 17, 0, 0))

    def test_parse_time_with_timeslot_aligment_intervals_add(self):
        """Test the create_aligned_datetime_var function."""
        # Run
        filepattern = "{start_time:%Y%m%d%H%M%S|align(15,0,1)}"
        result = create_aligned_datetime_var(filepattern,
                                             {'start_time':
                                              datetime(2015, 1, 9, 16, 59, 0)})
        # Assert
        self.assertEqual(result, datetime(2015, 1, 9, 17, 0, 0))

        # Run
        filepattern = "{start_time:%Y%m%d%H%M%S|align(15,0,2)}"
        result = create_aligned_datetime_var(filepattern,
                                             {'start_time':
                                              datetime(2015, 1, 9, 16, 59, 0)})
        # Assert
        self.assertEqual(result, datetime(2015, 1, 9, 17, 15, 0))

        # Run
        filepattern = "{start_time:%Y%m%d%H%M%S|align(15,0,-1)}"
        result = create_aligned_datetime_var(filepattern,
                                             {'start_time':
                                              datetime(2015, 1, 9, 16, 59, 0)})
        # Assert
        self.assertEqual(result, datetime(2015, 1, 9, 16, 30, 0))


def suite():
    """Test suite."""
    loader = unittest.TestLoader()
    mysuite = unittest.TestSuite()
    mysuite.addTest(loader.loadTestsFromTestCase(TestTimeUtilities))

    return mysuite


if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=2).run(suite())
