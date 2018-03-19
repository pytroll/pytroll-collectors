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
import logging

from pytroll_collectors.segments import SegmentGatherer, ini_to_dict
from pytroll_collectors.helper_functions import read_yaml

from pytroll_collectors.segments import (SLOT_NOT_READY,
                                         SLOT_NONCRITICAL_NOT_READY,
                                         SLOT_READY,
                                         SLOT_READY_BUT_WAIT_FOR_MORE,
                                         SLOT_OBSOLETE_TIMEOUT)


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_SINGLE = read_yaml(os.path.join(THIS_DIR, "data/segments_single.yaml"))
# CONFIG_DOUBLE = read_yaml(os.path.join(THIS_DIR, "data/segments_double.yaml"))
# CONFIG_INI = ini_to_dict(os.path.join(THIS_DIR, "data/segments.ini", "msg"))


class TestSegmentGatherer(unittest.TestCase):

    def setUp(self):
        """Setting up the testing
        """
        self.mda = {"segment": "EPI", "uid": "H-000-MSG3__-MSG3________-_________-EPI______-201611281100-__", "orig_platform_name": "MSG3", "start_time": dt.datetime(2016, 11, 28, 11, 0, 0), "nominal_time": dt.datetime(
            2016, 11, 28, 11, 0, 0), "uri": "/home/lahtinep/data/satellite/geo/msg/H-000-MSG3__-MSG3________-_________-EPI______-201611281100-__", "platform_name": "Meteosat-10", "channel_name": "", "path": "", "sensor": ["seviri"]}

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

    def test_init_data(self):
        mda = self.mda.copy()
        self.gyml1._init_data(mda)

        slot_str = str(mda["start_time"])
        self.assertEqual(self.gyml1.slots.keys()[0], slot_str)
        slot = self.gyml1.slots[slot_str]
        for key in mda:
            self.assertEqual(slot['metadata'][key], mda[key])
        self.assertEqual(slot['timeout'], None)
        self.assertEqual(slot['msg']['is_critical_set'],
                         CONFIG_SINGLE['patterns']['msg']['is_critical_set'])
        self.assertTrue('critical_files' in slot['msg'])
        self.assertTrue('wanted_files' in slot['msg'])
        self.assertTrue('all_files' in slot['msg'])
        self.assertTrue('received_files' in slot['msg'])
        self.assertTrue('delayed_files' in slot['msg'])
        self.assertTrue('missing_files' in slot['msg'])
        self.assertEqual(slot['msg']['files_till_premature_publish'], -1)

        self.assertEqual(len(slot['msg']['critical_files']), 2)
        self.assertEqual(len(slot['msg']['wanted_files']), 10)
        self.assertEqual(len(slot['msg']['all_files']), 10)

    def test_compose_filenames(self):
        mda = self.mda.copy()
        self.gyml1._init_data(mda)
        slot_str = str(mda["start_time"])
        fname_set = self.gyml1._compose_filenames(
            'msg', slot_str,
            self.gyml1._config['patterns']['msg']['critical_files'])
        self.assertTrue(fname_set, set)
        self.assertEqual(len(fname_set), 2)
        self.assertTrue("H-000-MSG3__-MSG3________-_________-PRO______-201611281100-__" in fname_set)
        self.assertTrue("H-000-MSG3__-MSG3________-_________-EPI______-201611281100-__" in fname_set)
        fname_set = self.gyml1._compose_filenames('msg', slot_str, None)
        self.assertTrue("H-000-MSG3__-MSG3________-_________-_________-201611281100-__" in fname_set)
        # Check that MSG segments can be given as range, and the
        # result is same as with explicit segment names
        fname_set_range = self.gyml1._compose_filenames(
            'msg', slot_str,
            self.gyml1._config['patterns']['msg']['wanted_files'])
        fname_set_explicit = self.gyml1._compose_filenames(
            'msg', slot_str,
            self.gyml1._config['patterns']['msg']['all_files'])
        self.assertEqual(len(fname_set_range), len(fname_set_explicit))
        self.assertEqual(len(fname_set_range.difference(fname_set_explicit)), 0)

        # TODO: Test *variable_tags*

    def test_set_logger(self):
        logger = logging.getLogger('foo')
        self.gyml1.set_logger(logger)
        self.assertTrue(logger is self.gyml1.logger)

    def test_update_timeout(self):
        mda = self.mda.copy()
        slot_str = str(mda["start_time"])
        self.gyml1._init_data(mda)
        now = dt.datetime.utcnow()
        self.gyml1.update_timeout(slot_str)
        diff = self.gyml1.slots[slot_str]['timeout'] - now
        self.assertAlmostEqual(diff.total_seconds(), 10.0, places=3)

    def test_slot_ready(self):
        mda = self.mda.copy()
        slot_str = str(mda["start_time"])
        # TODO

    def test_get_collection_status(self):
        mda = self.mda.copy()
        slot_str = str(mda["start_time"])

        now = dt.datetime.utcnow()
        future = now + dt.timedelta(minutes=1)
        past = now - dt.timedelta(minutes=1)
        func = self.gyml1.get_collection_status

        status = {}
        self.assertEqual(func(status, future, slot_str), SLOT_NOT_READY)

        status = {'foo': SLOT_NOT_READY}
        self.assertEqual(func(status, past, slot_str), SLOT_OBSOLETE_TIMEOUT)
        self.assertEqual(func(status, future, slot_str), SLOT_NOT_READY)

        status = {'foo': SLOT_NONCRITICAL_NOT_READY}
        self.assertEqual(func(status, past, slot_str), SLOT_OBSOLETE_TIMEOUT)
        self.assertEqual(func(status, future, slot_str),
                         SLOT_NONCRITICAL_NOT_READY)

        status = {'foo': SLOT_READY}
        self.assertEqual(func(status, past, slot_str), SLOT_READY)
        self.assertEqual(func(status, future, slot_str), SLOT_READY)

        status = {'foo': SLOT_READY_BUT_WAIT_FOR_MORE}
        self.assertEqual(func(status, past, slot_str), SLOT_READY)
        self.assertEqual(func(status, future, slot_str),
                         SLOT_READY_BUT_WAIT_FOR_MORE)

        # More than one fileset

        status = {'foo': SLOT_NOT_READY, 'bar': SLOT_NOT_READY}
        self.assertEqual(func(status, past, slot_str), SLOT_OBSOLETE_TIMEOUT)
        self.assertEqual(func(status, future, slot_str), SLOT_NOT_READY)

        status = {'foo': SLOT_NOT_READY, 'bar': SLOT_NONCRITICAL_NOT_READY}
        self.assertEqual(func(status, past, slot_str), SLOT_OBSOLETE_TIMEOUT)
        self.assertEqual(func(status, future, slot_str), SLOT_NOT_READY)

        status = {'foo': SLOT_NOT_READY, 'bar': SLOT_READY}
        self.assertEqual(func(status, past, slot_str), SLOT_OBSOLETE_TIMEOUT)
        self.assertEqual(func(status, future, slot_str), SLOT_NOT_READY)

        status = {'foo': SLOT_NOT_READY, 'bar': SLOT_READY_BUT_WAIT_FOR_MORE}
        self.assertEqual(func(status, past, slot_str), SLOT_OBSOLETE_TIMEOUT)
        self.assertEqual(func(status, future, slot_str), SLOT_NOT_READY)

        status = {'foo': SLOT_NONCRITICAL_NOT_READY,
                  'bar': SLOT_NONCRITICAL_NOT_READY}
        self.assertEqual(func(status, past, slot_str), SLOT_OBSOLETE_TIMEOUT)
        self.assertEqual(func(status, future, slot_str),
                         SLOT_NONCRITICAL_NOT_READY)

        status = {'foo': SLOT_NONCRITICAL_NOT_READY, 'bar': SLOT_READY}
        self.assertEqual(func(status, past, slot_str), SLOT_READY)
        self.assertEqual(func(status, future, slot_str),
                         SLOT_NONCRITICAL_NOT_READY)

        status = {'foo': SLOT_NONCRITICAL_NOT_READY,
                  'bar': SLOT_READY_BUT_WAIT_FOR_MORE}
        self.assertEqual(func(status, past, slot_str), SLOT_READY)
        self.assertEqual(func(status, future, slot_str),
                         SLOT_NONCRITICAL_NOT_READY)

        status = {'foo': SLOT_READY, 'bar': SLOT_READY}
        self.assertEqual(func(status, past, slot_str), SLOT_READY)
        self.assertEqual(func(status, future, slot_str), SLOT_READY)

        status = {'foo': SLOT_READY, 'bar': SLOT_READY_BUT_WAIT_FOR_MORE}
        self.assertEqual(func(status, past, slot_str), SLOT_READY)
        self.assertEqual(func(status, future, slot_str),
                         SLOT_READY_BUT_WAIT_FOR_MORE)

        status = {'foo': SLOT_READY_BUT_WAIT_FOR_MORE,
                  'bar': SLOT_READY_BUT_WAIT_FOR_MORE}
        self.assertEqual(func(status, past, slot_str), SLOT_READY)
        self.assertEqual(func(status, future, slot_str),
                         SLOT_READY_BUT_WAIT_FOR_MORE)

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
