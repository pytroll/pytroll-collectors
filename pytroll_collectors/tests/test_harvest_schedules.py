#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2019 - 2021 Pytroll developers
#
# Author(s):
#
#   Trygve Aspenes <trygveas@met.no>
#   Panu Lahtinen <panu.lahtinen@fmi.fi>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Unit testing some general purpose helper functions."""

import unittest
from unittest import mock
from tempfile import mkdtemp
import datetime
from pytroll_collectors.harvest_schedules import harvest_schedules, _generate_pass_list_file_name, _parse_schedules
import os
import shutil

import pytest
import logging


class FakeResponse:
    def __init__(self, data):
        self.data = data.split("\n")

    def readlines(self):
        self.status = 200 if self.data is not None else 404
        return self.data

    def close(self):
        pass


fake_test_pass_file = """
EARS-VIIRS Pass Predictions
2019/12/16 05:08:01
scheduleBeginEumetsat,scheduleEndEumetsat,Satellite
2019-12-16 00:24,2019-12-16 00:51,npp
2019-12-16 01:14,2019-12-16 01:44,noaa20
2019-12-16 02:05,2019-12-16 02:35,npp
2019-12-16 02:55,2019-12-16 03:25,noaa20
2019-12-16 03:46,2019-12-16 04:13,npp
2019-12-16 04:36,2019-12-16 04:56,noaa20
2019-12-16 05:26,2019-12-16 05:47,npp
2019-12-16 06:16,2019-12-16 06:37,noaa20
2019-12-16 07:05,2019-12-16 07:27,npp
2019-12-16 07:55,2019-12-16 08:16,noaa20
2019-12-16 08:46,2019-12-16 09:06,npp
2019-12-16 09:29,2019-12-16 09:55,noaa20
2019-12-16 10:17,2019-12-16 10:44,npp
2019-12-16 11:06,2019-12-16 11:34,noaa20
2019-12-16 11:57,2019-12-16 12:24,npp
2019-12-16 12:48,2019-12-16 13:15,noaa20
2019-12-16 13:37,2019-12-16 14:05,npp
2019-12-16 14:26,2019-12-16 14:56,noaa20
2019-12-16 15:18,2019-12-16 15:47,npp
2019-12-16 16:19,2019-12-16 16:37,noaa20
2019-12-16 17:10,2019-12-16 17:30,npp
2019-12-16 18:01,2019-12-16 18:20,noaa20
2019-12-16 18:55,2019-12-16 19:12,npp
2019-12-16 19:51,2019-12-16 20:03,noaa20
2019-12-16 20:42,2019-12-16 20:56,npp
2019-12-16 21:33,2019-12-16 21:47,noaa20
2019-12-16 22:24,2019-12-16 22:50,npp
2019-12-16 23:14,2019-12-16 23:41,noaa20
"""


class TestHarvestSchedules(unittest.TestCase):
    """Harvest schedule tests."""

    def setUp(self):
        self.basedir = mkdtemp()

    def tearDown(self):
        try:
            shutil.rmtree(self.basedir, ignore_errors=True)
        except OSError:
            pass

    def test_pass_list_file_name(self):
        granule_metadata = {
            'origin': '157.249.16.188:9063', 'sensor': ['viirs'], 'end_decimal': 3,
            'stream': 'eumetcast', 'format': 'SDR_compact', 'orig_platform_name': 'npp',
            'start_time': datetime.datetime(2019, 12, 16, 13, 44, 9), 'variant': 'EARS',
            'orbit_number': 42154, 'dataset':
            [{'uri':
              '/data/pytroll/VIIRS-EARS/SVMC_npp_d20191216_t1344091_e1345333_b42154_c20191216135112000242_eum_ops.h5',
              'uid': 'SVMC_npp_d20191216_t1344091_e1345333_b42154_c20191216135112000242_eum_ops.h5'}],
            'start_decimal': 1, 'proctime': '20191216135112000242', 'antenna': 'ears',
            'data_processing_level': '1B', 'tle_platform_name': 'SUOMI NPP', 'platform_name': 'npp',
            'end_time': datetime.datetime(2019, 12, 16, 13, 45, 33), 'type': 'HDF5',
            'collection_area_id': 'eurol'}

        planned_granule_times = set([datetime.datetime(2019, 12, 13, 13, 19),
                                     datetime.datetime(2019, 12, 13, 13, 38),
                                     datetime.datetime(2019, 12, 13, 13, 27)])

        params = {'granule_metadata': granule_metadata,
                  'planned_granule_times': planned_granule_times}
        eum, save_file = _generate_pass_list_file_name(params, self.basedir, 'https://uns.eumetsat.int/downloads/ears/')
        self.assertEqual('https://uns.eumetsat.int/downloads/ears/ears_viirs_pass_prediction_19-12-16.txt', eum)
        self.assertEqual(os.path.join(self.basedir, 'ears_viirs_pass_prediction_19-12-16.txt'), save_file)

    def test_pass_list_no_sensor(self):
        granule_metadata = {
            'origin': '157.249.16.188:9063', 'end_decimal': 3,
            'stream': 'eumetcast', 'format': 'SDR_compact', 'orig_platform_name': 'npp',
            'start_time': datetime.datetime(2019, 12, 16, 13, 44, 9), 'variant': 'EARS',
            'orbit_number': 42154, 'dataset':
            [{'uri':
              '/data/pytroll/VIIRS-EARS/SVMC_npp_d20191216_t1344091_e1345333_b42154_c20191216135112000242_eum_ops.h5',
              'uid': 'SVMC_npp_d20191216_t1344091_e1345333_b42154_c20191216135112000242_eum_ops.h5'}],
            'start_decimal': 1, 'proctime': '20191216135112000242', 'antenna': 'ears',
            'data_processing_level': '1B', 'tle_platform_name': 'SUOMI NPP', 'platform_name': 'npp',
            'end_time': datetime.datetime(2019, 12, 16, 13, 45, 33), 'type': 'HDF5',
            'collection_area_id': 'eurol'}

        planned_granule_times = set([datetime.datetime(2019, 12, 13, 13, 19),
                                     datetime.datetime(2019, 12, 13, 13, 38),
                                     datetime.datetime(2019, 12, 13, 13, 27)])

        params = {'granule_metadata': granule_metadata,
                  'planned_granule_times': planned_granule_times}
        eum, save_file = _generate_pass_list_file_name(params, self.basedir, 'https://uns.eumetsat.int/downloads/ears/')
        self.assertIsNone(eum)
        self.assertIsNone(save_file)

    @mock.patch('pytroll_collectors.harvest_schedules.urlopen', return_value=FakeResponse(data=fake_test_pass_file))
    def test_harvest_schedule_2(self, mock_harvest_schedules_urlopen):
        granule_metadata = {
            'origin': '157.249.16.188:9063', 'sensor': ['viirs'], 'end_decimal': 3, 'stream': 'eumetcast',
            'format': 'SDR_compact', 'orig_platform_name': 'npp',
            'start_time': datetime.datetime(2019, 12, 16, 13, 44, 9), 'variant': 'EARS', 'orbit_number': 42154,
            'dataset':
            [{'uri':
              '/data/pytroll/VIIRS-EARS/SVMC_npp_d20191216_t1344091_e1345333_b42154_c20191216135112000242_eum_ops.h5',
              'uid': 'SVMC_npp_d20191216_t1344091_e1345333_b42154_c20191216135112000242_eum_ops.h5'}],
            'start_decimal': 1, 'proctime': '20191216135112000242', 'antenna': 'ears', 'data_processing_level': '1B',
            'tle_platform_name': 'SUOMI NPP', 'platform_name': 'npp',
            'end_time': datetime.datetime(2019, 12, 16, 13, 45, 33), 'type': 'HDF5', 'collection_area_id': 'eurol'}

        planned_granule_times = set([datetime.datetime(2019, 12, 13, 13, 19),
                                     datetime.datetime(2019, 12, 13, 13, 38),
                                     datetime.datetime(2019, 12, 13, 13, 27)])
        params = {'granule_metadata': granule_metadata,
                  'planned_granule_times': planned_granule_times}

        harvest_schedules(params, save_basename=self.basedir)
        self.assertEqual(mock_harvest_schedules_urlopen.call_count, 1)

    @mock.patch('pytroll_collectors.harvest_schedules.urlopen')
    def test_harvest_schedule(self, mock_harvest_schedules):
        mock_harvest_schedules.readline.side_effect = 'test2'
        # self.cloud_image.fetch()
        granule_metadata = {
            'origin': '157.249.16.188:9063', 'sensor': ['viirs'], 'end_decimal': 3, 'stream': 'eumetcast',
            'format': 'SDR_compact', 'orig_platform_name': 'npp',
            'start_time': datetime.datetime(2019, 12, 16, 13, 44, 9), 'variant': 'EARS', 'orbit_number': 42154,
            'dataset':
            [{'uri':
              '/data/pytroll/VIIRS-EARS/SVMC_npp_d20191216_t1344091_e1345333_b42154_c20191216135112000242_eum_ops.h5',
              'uid': 'SVMC_npp_d20191216_t1344091_e1345333_b42154_c20191216135112000242_eum_ops.h5'}],
            'start_decimal': 1, 'proctime': '20191216135112000242', 'antenna': 'ears', 'data_processing_level': '1B',
            'tle_platform_name': 'SUOMI NPP', 'platform_name': 'npp',
            'end_time': datetime.datetime(2019, 12, 16, 13, 45, 33), 'type': 'HDF5', 'collection_area_id': 'eurol'}

        planned_granule_times = set([datetime.datetime(2019, 12, 13, 13, 19),
                                     datetime.datetime(2019, 12, 13, 13, 38),
                                     datetime.datetime(2019, 12, 13, 13, 27)])

        params = {'granule_metadata': granule_metadata,
                  'planned_granule_times': planned_granule_times}

        harvest_schedules(params, save_basename=self.basedir)
        self.assertEqual(mock_harvest_schedules.call_count, 1)

        # Do a second call, should reread the file instead of download.
        min_time, max_time = harvest_schedules(params, save_basename=self.basedir)
        self.assertIsNone(min_time)
        self.assertIsNone(max_time)

    @pytest.fixture(autouse=True)
    def inject_fixtures(self, caplog):
        """Inject fixtures."""
        self._caplog = caplog

    @mock.patch('pytroll_collectors.harvest_schedules.urlopen')
    def test_harvest_schedule_HTTPError(self, mock_harvest_schedules):
        import sys
        from contextlib import contextmanager
        from io import StringIO
        from urllib.error import HTTPError
        mock_harvest_schedules.side_effect = HTTPError('This failed', 0, '', '', None)
        # self.cloud_image.fetch()
        granule_metadata = {
            'origin': '157.249.16.188:9063', 'sensor': ['viirs'], 'end_decimal': 3, 'stream': 'eumetcast',
            'format': 'SDR_compact', 'orig_platform_name': 'npp',
            'start_time': datetime.datetime(2019, 12, 16, 13, 44, 9), 'variant': 'EARS', 'orbit_number': 42154,
            'dataset':
            [{'uri':
              '/data/pytroll/VIIRS-EARS/SVMC_npp_d20191216_t1344091_e1345333_b42154_c20191216135112000242_eum_ops.h5',
              'uid': 'SVMC_npp_d20191216_t1344091_e1345333_b42154_c20191216135112000242_eum_ops.h5'}],
            'start_decimal': 1, 'proctime': '20191216135112000242', 'antenna': 'ears', 'data_processing_level': '1B',
            'tle_platform_name': 'SUOMI NPP', 'platform_name': 'npp',
            'end_time': datetime.datetime(2019, 12, 16, 13, 45, 33), 'type': 'HDF5', 'collection_area_id': 'eurol'}

        planned_granule_times = set([datetime.datetime(2019, 12, 13, 13, 19),
                                     datetime.datetime(2019, 12, 13, 13, 38),
                                     datetime.datetime(2019, 12, 13, 13, 27)])

        params = {'granule_metadata': granule_metadata,
                  'planned_granule_times': planned_granule_times}

        self._caplog.propagate = True
        with self._caplog.at_level(logging.ERROR):
            min_time, max_time = harvest_schedules(params, save_basename=self.basedir)        
            logs = str([rec.message for rec in self._caplog.records])
            self.assertIsNone(min_time)
            self.assertIsNone(max_time)
            self.assertIn("Failed to download file:", logs)

    def test_parse_schedules(self):
        granule_metadata = {
            'origin': '157.249.16.188:9063', 'sensor': ['viirs'], 'end_decimal': 3, 'stream': 'eumetcast',
            'format': 'SDR_compact', 'orig_platform_name': 'npp',
            'start_time': datetime.datetime(2019, 12, 16, 13, 44, 9), 'variant': 'EARS', 'orbit_number': 42154,
            'dataset':
            [{'uri':
              '/data/pytroll/VIIRS-EARS/SVMC_npp_d20191216_t1344091_e1345333_b42154_c20191216135112000242_eum_ops.h5',
              'uid': 'SVMC_npp_d20191216_t1344091_e1345333_b42154_c20191216135112000242_eum_ops.h5'}],
            'start_decimal': 1, 'proctime': '20191216135112000242', 'antenna': 'ears', 'data_processing_level': '1B',
            'tle_platform_name': 'SUOMI NPP', 'platform_name': 'suomi npp',
            'end_time': datetime.datetime(2019, 12, 16, 13, 45, 33), 'type': 'HDF5', 'collection_area_id': 'eurol'}

        planned_granule_times = set([datetime.datetime(2019, 12, 16, 13, 41, 18, 200000),
                                     datetime.datetime(2019, 12, 16, 13, 42, 43, 600000),
                                     datetime.datetime(2019, 12, 16, 13, 44, 9),
                                     datetime.datetime(2019, 12, 16, 13, 45, 34, 400000),
                                     datetime.datetime(2019, 12, 16, 13, 46, 59, 800000),
                                     datetime.datetime(2019, 12, 16, 13, 48, 25, 200000),
                                     datetime.datetime(2019, 12, 16, 13, 49, 50, 600000),
                                     datetime.datetime(2019, 12, 16, 13, 51, 16),
                                     datetime.datetime(2019, 12, 16, 13, 52, 41, 400000),
                                     datetime.datetime(2019, 12, 16, 13, 54, 6, 800000),
                                     datetime.datetime(2019, 12, 16, 13, 55, 32, 200000),
                                     datetime.datetime(2019, 12, 16, 13, 56, 57, 600000)])
        params = {'granule_metadata': granule_metadata,
                  'planned_granule_times': planned_granule_times}
        min_times, max_times = _parse_schedules(params, fake_test_pass_file.split("\n"))
        self.assertEqual(min_times, datetime.datetime(2019, 12, 16, 13, 37))
        self.assertEqual(max_times, datetime.datetime(2019, 12, 16, 14, 5))


def suite():
    """Test suite."""
    loader = unittest.TestLoader()
    mysuite = unittest.TestSuite()
    mysuite.addTest(loader.loadTestsFromTestCase(TestHarvestSchedules))

    return mysuite


if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=2).run(suite())
