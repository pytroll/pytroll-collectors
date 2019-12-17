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
from unittest.mock import Mock, patch

try:
    from urllib.request import urlopen
except ImportError:
    from urllib import urlopen

import datetime
from pytroll_collectors.harvest_schedules import harvest_schedules


class TestHarvestSchedules(unittest.TestCase):
    """Harvest schedule tests."""

    @patch('pytroll_collectors.harvest_schedules.urlopen')
    def test_harvest_schedule(self, mock_harvest_schedules):
        #m_urlopen().read.side_effect = ["foo", "bar", ""]
        #mock_harvest_schedules.return_value = 'test'
        mock_harvest_schedules.readline.side_effect = 'test2'
        # self.cloud_image.fetch()
        granule_metadata = {'origin': '157.249.16.182:9223', 'polarization': 'DH', 'product_class': 'S', 'sar_mode': 'IW', 'start_time': datetime.datetime(2019, 12, 16, 9, 12, 22), 'orbit_number': 19388, 'uri': '/data/pytroll/colhub-ocn/S1B_IW_OCN__2SDH_20191216T091222_20191216T091247_019388_0249F6_E886.SAFE.zip', 'platform_name': 'Sentinel-1B', 'end_time': datetime.datetime(2019, 12, 16, 9, 12, 47), 'product_unique_id': 'E886', 'tle_platform_name': 'SENTINEL 1B', 'mission_data_take_id': '0249F6', 'sensor': 'sar', 'processing_level': '2', 'uid': 'S1B_IW_OCN__2SDH_20191216T091222_20191216T091247_019388_0249F6_E886.SAFE.zip', 'collection_area_id': 'eurol'}

        planned_granule_times = set([datetime.datetime(2019, 12, 13, 13, 19),
                                     datetime.datetime(2019, 12, 13, 13, 38),
                                     datetime.datetime(2019, 12, 13, 13, 27)])

        params = {'granule_metadata': granule_metadata,
                  'planned_granule_times': planned_granule_times}
        harvest_schedules(params)


def suite():
    """Test suite."""
    loader = unittest.TestLoader()
    mysuite = unittest.TestSuite()
    mysuite.addTest(loader.loadTestsFromTestCase(TestHarvestSchedules))

    return mysuite


if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=2).run(suite())
