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

"""Unit testing for segment gatherer."""

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
CONFIG_DOUBLE = read_yaml(os.path.join(THIS_DIR, "data/segments_double.yaml"))
CONFIG_DOUBLE_DIFFERENT = read_yaml(os.path.join(THIS_DIR, "data/segments_double_different.yaml"))
CONFIG_NO_SEG = read_yaml(os.path.join(THIS_DIR,
                                       "data/segments_double_no_segments.yaml"))
CONFIG_INI = ini_to_dict(os.path.join(THIS_DIR, "data/segments.ini"), "msg")
CONFIG_INI_NO_SEG = ini_to_dict(os.path.join(THIS_DIR, "data/segments.ini"),
                                "goes16")
CONFIG_INI_HIMAWARI = ini_to_dict(os.path.join(THIS_DIR, "data/segments.ini"),
                                  "himawari-8")


class TestSegmentGatherer(unittest.TestCase):
    """Tests for the segment gatherer."""

    def setUp(self):
        """Set up the testing."""
        self.mda_msg0deg = {"segment": "EPI", "uid": "H-000-MSG3__-MSG3________-_________-EPI______-201611281100-__", "platform_shortname": "MSG3", "start_time": dt.datetime(2016, 11, 28, 11, 0, 0), "nominal_time": dt.datetime(  # noqa
            2016, 11, 28, 11, 0, 0), "uri": "/home/lahtinep/data/satellite/geo/msg/H-000-MSG3__-MSG3________-_________-EPI______-201611281100-__", "platform_name": "Meteosat-10", "channel_name": "", "path": "", "sensor": ["seviri"], "hrit_format": "MSG3"}  # noqa

        self.mda_iodc = {"segment": "EPI", "uid": "H-000-MSG2__-MSG2_IODC___-_________-EPI______-201611281100-__", "platform_shortname": "MSG2", "start_time": dt.datetime(2016, 11, 28, 11, 0, 0), "nominal_time": dt.datetime(  # noqa
            2016, 11, 28, 11, 0, 0), "uri": "/home/lahtinep/data/satellite/geo/msg/H-000-MSG2__-MSG2_IODC___-_________-EPI______-201611281100-__", "platform_name": "Meteosat-9", "channel_name": "", "path": "", "sensor": ["seviri"]}  # noqa

        self.mda_pps = {"end_tenths": 4, "uid": "S_NWC_CMA_metopb_28538_20180319T0955387Z_20180319T1009544Z.nc", "platform_shortname": "metopb", "start_time": dt.datetime(2018, 3, 19, 9, 55, 0), "start_tenths": 7, "orbit_number": 28538, "uri":  # noqa
                        "/home/lahtinep/data/satellite/hrpt-pps/S_NWC_CMA_metopb_28538_20180319T0955387Z_20180319T1009544Z.nc", "start_seconds": 38, "platform_name": "Metop-B", "end_seconds": 54, "end_time": dt.datetime(2018, 3, 19, 10, 9, 0), "path": "", "sensor": ["avhrr/3"]}  # noqa

        self.mda_hrpt = {"uid": "hrpt_metop01_20180319_0955_28538.l1b", "platform_shortname": "metop01", "start_time": dt.datetime(2018, 3, 19, 9, 55, 0), "orbit_number":  # noqa
                         28538, "uri": "/home/lahtinep/data/satellite/new/hrpt_metop01_20180319_0955_28538.l1b", "platform_name": "Metop-B", "path": "", "sensor": ["avhrr/3"]}  # noqa

        self.mda_goes16 = {"uid": "OR_ABI-L1b-RadF-M3C08_G16_s20190320600324_e20190320611091_c20190320611138.nc",
                           "creation_time": "20190320611138",
                           "start_time": dt.datetime(2019, 2, 1, 6, 0, 0),
                           "area_code": "F", "mission_id": "ABI", "path": "",
                           "system_environment": "OR", "scan_mode": "M3",
                           "uri": "/path/OR_ABI-L1b-RadF-M3C08_G16_s20190320600324_e20190320611091_c20190320611138.nc",
                           "start_seconds": dt.datetime(1900, 1, 1, 0, 0, 32, 400000),
                           "platform_name": "GOES-16",
                           "end_time": dt.datetime(2019, 2, 1, 6, 11, 9, 100000),
                           "orig_platform_name": "G16", "dataset_name": "Rad",
                           "sensor": ["abi"], "channel": "C08"}

        self.msg0deg = SegmentGatherer(CONFIG_SINGLE)
        self.msg0deg_iodc = SegmentGatherer(CONFIG_DOUBLE)
        self.iodc_himawari = SegmentGatherer(CONFIG_DOUBLE_DIFFERENT)
        self.hrpt_pps = SegmentGatherer(CONFIG_NO_SEG)
        self.msg_ini = SegmentGatherer(CONFIG_INI)
        self.goes_ini = SegmentGatherer(CONFIG_INI_NO_SEG)
        self.himawari_ini = SegmentGatherer(CONFIG_INI_HIMAWARI)

    def test_init(self):
        """Test init."""
        self.assertTrue(self.msg0deg._config == CONFIG_SINGLE)
        self.assertTrue(self.msg0deg._subject is None)
        self.assertEqual(list(self.msg0deg._patterns.keys()), ['msg'])
        self.assertEqual(list(self.msg0deg._parsers.keys()), ['msg'])
        self.assertEqual(len(self.msg0deg.slots.keys()), 0)
        self.assertEqual(self.msg0deg.time_name, 'start_time')
        self.assertFalse(self.msg0deg._loop)
        self.assertEqual(self.msg0deg._time_tolerance, 30)
        self.assertEqual(self.msg0deg._timeliness.total_seconds(), 10)
        self.assertEqual(self.msg0deg._listener, None)
        self.assertEqual(self.msg0deg._publisher, None)

        # Tests using two filesets start_time_pattern
        self.assertTrue('start_time_pattern' in self.msg0deg_iodc._patterns['msg'])
        self.assertTrue('_start_time_pattern' in self.msg0deg_iodc._patterns['msg'])
        self.assertTrue('start_time_pattern' in self.msg0deg_iodc._patterns['iodc'])
        self.assertTrue('_start_time_pattern' in self.msg0deg_iodc._patterns['iodc'])

    def test_init_data(self):
        """Test initializing the data."""
        mda = self.mda_msg0deg.copy()
        self.msg0deg._init_data(mda)

        slot_str = str(mda["start_time"])
        self.assertEqual(list(self.msg0deg.slots.keys())[0], slot_str)
        slot = self.msg0deg.slots[slot_str]
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

        # Tests using two filesets
        self.msg0deg_iodc._init_data(mda)
        slot = self.msg0deg_iodc.slots[slot_str]
        self.assertTrue('collection' in slot['metadata'])
        for key in self.msg0deg_iodc._patterns:
            self.assertTrue('dataset' in slot['metadata']['collection'][key])
            self.assertTrue('sensor' in slot['metadata']['collection'][key])

        # Test using .ini config
        self.msg_ini._init_data(mda)
        slot = self.msg_ini.slots[slot_str]
        self.assertEqual(len(slot['msg']['critical_files']), 2)
        self.assertEqual(len(slot['msg']['wanted_files']), 38)
        self.assertEqual(len(slot['msg']['all_files']), 114)

    def test_compose_filenames(self):
        """Test composing the filenames."""
        mda = self.mda_msg0deg.copy()
        self.msg0deg._init_data(mda)
        slot_str = str(mda["start_time"])
        fname_set = self.msg0deg._compose_filenames(
            'msg', slot_str,
            self.msg0deg._config['patterns']['msg']['critical_files'])
        self.assertTrue(fname_set, set)
        self.assertEqual(len(fname_set), 2)
        self.assertTrue("H-000-MSG3__-MSG3________-_________-PRO______-201611281100-__" in fname_set)
        self.assertTrue("H-000-MSG3__-MSG3________-_________-EPI______-201611281100-__" in fname_set)
        fname_set = self.msg0deg._compose_filenames('msg', slot_str, None)
        self.assertEqual(len(fname_set), 0)
        # Check that MSG segments can be given as range, and the
        # result is same as with explicit segment names
        fname_set_range = self.msg0deg._compose_filenames(
            'msg', slot_str,
            self.msg0deg._config['patterns']['msg']['wanted_files'])
        fname_set_explicit = self.msg0deg._compose_filenames(
            'msg', slot_str,
            self.msg0deg._config['patterns']['msg']['all_files'])
        self.assertEqual(len(fname_set_range), len(fname_set_explicit))
        self.assertEqual(len(fname_set_range.difference(fname_set_explicit)), 0)

        # Tests using filesets with no segments
        mda = self.mda_hrpt.copy()
        self.hrpt_pps._init_data(mda)
        slot_str = str(mda["start_time"])
        fname_set = self.hrpt_pps._compose_filenames(
            'hrpt', slot_str,
            self.hrpt_pps._config['patterns']['hrpt']['critical_files'])
        self.assertEqual(len(fname_set), 1)
        self.assertTrue("hrpt_*_20180319_0955_28538.l1b" in fname_set)
        fname_set = self.hrpt_pps._compose_filenames(
            'pps', slot_str,
            self.hrpt_pps._config['patterns']['pps']['critical_files'])
        self.assertEqual(len(fname_set), 1)
        self.assertTrue(
            "S_NWC_CMA_*_28538_20180319T0955???Z_????????T???????Z.nc" in fname_set)

        # Tests using filesets with no segments, INI config
        mda = self.mda_goes16.copy()
        self.goes_ini._init_data(mda)
        slot_str = str(mda["start_time"])
        fname_set = self.goes_ini._compose_filenames(
            'goes16', slot_str,
            self.goes_ini._config['patterns']['goes16']['critical_files'])
        self.assertEqual(len(fname_set), 0)

    def test_set_logger(self):
        """Test setting the logger."""
        logger = logging.getLogger('foo')
        self.msg0deg.set_logger(logger)
        self.assertTrue(logger is self.msg0deg.logger)

    def test_update_timeout(self):
        """Test updating the timeout."""
        mda = self.mda_msg0deg.copy()
        slot_str = str(mda["start_time"])
        self.msg0deg._init_data(mda)
        now = dt.datetime.utcnow()
        self.msg0deg.update_timeout(slot_str)
        diff = self.msg0deg.slots[slot_str]['timeout'] - now
        self.assertAlmostEqual(diff.total_seconds(), 10.0, places=3)

    def test_slot_ready(self):
        """Test if a slot is ready."""
        mda = self.mda_msg0deg.copy()
        slot_str = str(mda["start_time"])
        self.msg0deg._init_data(mda)
        func = self.msg0deg.slot_ready
        self.assertTrue(self.msg0deg.slots[slot_str]['timeout'] is None)
        res = func(slot_str)
        self.assertEqual(res, SLOT_NOT_READY)
        self.assertTrue(self.msg0deg.slots[slot_str]['timeout'] is not None)
        # TODO

    def test_get_collection_status(self):
        """Test getting the collection status."""
        mda = self.mda_msg0deg.copy()
        slot_str = str(mda["start_time"])

        now = dt.datetime.utcnow()
        future = now + dt.timedelta(minutes=1)
        past = now - dt.timedelta(minutes=1)
        func = self.msg0deg.get_collection_status

        status = {}
        self.assertEqual(func(status, future, slot_str), SLOT_NOT_READY)

        status = {'foo': SLOT_NOT_READY}
        self.assertEqual(func(status, past, slot_str), SLOT_OBSOLETE_TIMEOUT)
        self.assertEqual(func(status, future, slot_str), SLOT_NOT_READY)

        status = {'foo': SLOT_NONCRITICAL_NOT_READY}
        self.msg0deg.slots[slot_str] = {'foo': {'received_files': [0, 1]}}
        self.assertEqual(func(status, past, slot_str), SLOT_READY)
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
        self.assertEqual(func(status, past, slot_str), SLOT_READY)
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

    def test_add_file(self):
        """Test adding a file."""
        # Single fileset
        msg_data = self.mda_msg0deg.copy()
        col = self.msg0deg
        col._init_data(msg_data)
        time_slot = list(col.slots.keys())[0]
        key = list(CONFIG_SINGLE['patterns'].keys())[0]
        mda = col._parsers[key].parse(msg_data['uid'])
        res = col.add_file(time_slot, key, mda, msg_data)
        self.assertTrue(res is None)
        self.assertEqual(len(col.slots[time_slot][key]['received_files']), 1)
        meta = col.slots[time_slot]['metadata']
        self.assertEqual(len(meta['dataset']), 1)
        self.assertTrue('uri' in meta['dataset'][0])
        self.assertTrue('uid' in meta['dataset'][0])

        # Two filesets
        msg_data = {'msg': self.mda_msg0deg.copy(), 'iodc': self.mda_iodc.copy()}
        col = self.msg0deg_iodc
        col._init_data(msg_data['msg'])
        time_slot = str(msg_data['msg']['start_time'])
        i = 0
        for key in CONFIG_DOUBLE['patterns']:
            mda = col._parsers[key].parse(msg_data[key]['uid'])
            res = col.add_file(time_slot, key, mda, msg_data[key])
            self.assertTrue(res is None)
            self.assertEqual(len(col.slots[time_slot][key]['received_files']),
                             1)
            meta = col.slots[time_slot]['metadata']
            self.assertEqual(len(meta['collection'][key]['dataset']), 1)
            self.assertTrue('uri' in meta['collection'][key]['dataset'][0])
            self.assertTrue('uid' in meta['collection'][key]['dataset'][0])
            i += 1

        # Two filesets without segments
        msg_data = {'hrpt': self.mda_hrpt.copy(),
                    'pps': self.mda_pps.copy()}
        col = self.hrpt_pps
        col._init_data(msg_data['hrpt'])
        time_slot = str(msg_data['hrpt']['start_time'])
        i = 0
        for key in CONFIG_NO_SEG['patterns']:
            mda = col._parsers[key].parse(msg_data[key]['uid'])
            res = col.add_file(time_slot, key, mda, msg_data[key])
            self.assertTrue(res is None)
            self.assertEqual(len(col.slots[time_slot][key]['received_files']),
                             1)
            meta = col.slots[time_slot]['metadata']
            self.assertEqual(len(meta['collection'][key]['dataset']), 1)
            i += 1

    def test_ini_to_dict(self):
        """Test ini conversion to dict."""
        config = ini_to_dict(os.path.join(THIS_DIR, "data/segments.ini"), "msg")
        self.assertTrue('patterns' in config)
        self.assertTrue('posttroll' in config)
        self.assertTrue('time_tolerance' in config)
        self.assertTrue('timeliness' in config)
        self.assertTrue('num_files_premature_publish' in config)

        self.assertTrue('topics' in config['posttroll'])
        self.assertTrue('nameservers' in config['posttroll'])
        self.assertTrue('addresses' in config['posttroll'])
        self.assertTrue('topics' in config['posttroll'])
        self.assertTrue('publish_port' in config['posttroll'])
        self.assertTrue('publish_topic' in config['posttroll'])

        self.assertTrue('msg' in config['patterns'])
        self.assertTrue('pattern' in config['patterns']['msg'])
        self.assertTrue('critical_files' in config['patterns']['msg'])
        self.assertTrue('wanted_files' in config['patterns']['msg'])
        self.assertTrue('all_files' in config['patterns']['msg'])
        self.assertTrue('is_critical_set' in config['patterns']['msg'])
        self.assertTrue('variable_tags' in config['patterns']['msg'])

    def test_check_schedule_time(self):
        """Test Check Schedule Time."""
        import datetime

        hour = self.msg0deg_iodc._patterns['msg']['_start_time_pattern']
        self.assertTrue(self.msg0deg.check_schedule_time(hour, datetime.time(9, 0)))
        self.assertFalse(self.msg0deg.check_schedule_time(hour, datetime.time(9, 30)))
        self.assertFalse(self.msg0deg.check_schedule_time(hour, datetime.time(23, 0)))
        hour = self.msg0deg_iodc._patterns['iodc']['_start_time_pattern']
        self.assertTrue(self.msg0deg.check_schedule_time(hour, datetime.time(4, 15)))
        self.assertFalse(self.msg0deg.check_schedule_time(hour, datetime.time(4, 30)))
        self.assertFalse(self.msg0deg.check_schedule_time(hour, datetime.time(11, 0)))

    def test_floor_time(self):
        """Test that flooring the time to set minutes work."""
        parser = self.himawari_ini._parsers['himawari-8']
        mda = parser.parse("IMG_DK01IR4_201712081129_010")
        self.assertEqual(mda['start_time'].minute, 29)
        mda2 = self.himawari_ini._floor_time(mda.copy())
        self.assertEqual(mda2['start_time'].minute, 20)
        self.himawari_ini._group_by_minutes = 15
        mda2 = self.himawari_ini._floor_time(mda.copy())
        self.assertEqual(mda2['start_time'].minute, 15)
        self.himawari_ini._group_by_minutes = 2
        mda2 = self.himawari_ini._floor_time(mda.copy())
        self.assertEqual(mda2['start_time'].minute, 28)
        # Add seconds
        mod_mda = mda.copy()
        start_time = mda['start_time']
        mod_mda['start_time'] = dt.datetime(start_time.year, start_time.month,
                                            start_time.day, start_time.hour,
                                            start_time.minute, 42)
        # The seconds should also be zero'd
        mda2 = self.himawari_ini._floor_time(mda.copy())
        self.assertEqual(mda2['start_time'].minute, 28)
        self.assertEqual(mda2['start_time'].second, 0)

        # Test that nothing is changed when groub_by_minutes has not
        # been configured
        self.himawari_ini._group_by_minutes = None
        mda2 = self.himawari_ini._floor_time(mod_mda.copy())
        self.assertEqual(mda2['start_time'], mod_mda['start_time'])

    def test_floor_time_different(self):
        """Test that flooring the time to set minutes work."""
        key = 'himawari'
        parser = self.iodc_himawari._parsers[key]
        mda = parser.parse("IMG_DK01IR4_201712081129_010")
        self.assertEqual(mda['start_time'].minute, 29)

        # Here the floor time (group_by_minutes)is read from the yaml config file
        # specific for himawari. You dont want to group_by_minutes for IODC
        mda2 = self.iodc_himawari._floor_time(mda.copy(), key)
        self.assertEqual(mda2['start_time'].minute, 20)
        # Add seconds
        mod_mda = mda.copy()
        start_time = mda['start_time']
        mod_mda['start_time'] = dt.datetime(start_time.year, start_time.month,
                                            start_time.day, start_time.hour,
                                            start_time.minute, 42)
        # The seconds should also be zero'd ( group_by_minutes from config file)
        mda2 = self.iodc_himawari._floor_time(mda.copy(), key)
        self.assertEqual(mda2['start_time'].minute, 20)
        self.assertEqual(mda2['start_time'].second, 0)

        # Test that nothing is changed when groub_by_minutes has not
        # been configured
        self.iodc_himawari._group_by_minutes = None
        mda2 = self.iodc_himawari._floor_time(mod_mda.copy())
        self.assertEqual(mda2['start_time'], mod_mda['start_time'])

        key = 'iodc'
        parser = self.iodc_himawari._parsers[key]
        mda = parser.parse("H-000-MSG2__-MSG2_IODC___-_________-EPI______-201611281115-__")
        self.assertEqual(mda['start_time'].minute, 15)

        # Here the floor time (group_by_minutes)is read from the yaml config file
        # but it is not given for IODC
        mda2 = self.iodc_himawari._floor_time(mda.copy(), key)
        self.assertEqual(mda2['start_time'].minute, 15)

    def test_copy_metadata(self):
        """Test combining metadata from a message and parsed from filename."""
        from pytroll_collectors.segments import copy_metadata
        try:
            from unittest import mock
        except ImportError:
            import mock

        mda = {'a': 1, 'b': 2}
        msg = mock.MagicMock()
        msg.data = {'a': 2, 'c': 3}

        res = copy_metadata(mda, msg)
        self.assertEqual(res['a'], 2)
        self.assertEqual(res['b'], 2)
        self.assertEqual(res['c'], 3)

        # Keep 'a' from parsed metadata
        res = copy_metadata(mda, msg, keep_parsed_keys=['a'])
        self.assertEqual(res['a'], 1)
        self.assertEqual(res['b'], 2)
        self.assertEqual(res['c'], 3)

        # Keep 'a' from parsed metadata configured for one of more patterns
        res = copy_metadata(mda, msg, local_keep_parsed_keys=['a'])
        self.assertEqual(res['a'], 1)
        self.assertEqual(res['b'], 2)
        self.assertEqual(res['c'], 3)

    def test_publish_service_name(self):
        """Test publish service name. Need to be equal each time"""
        col = self.msg0deg_iodc
        publish_service_name = col._generate_publish_service_name()
        self.assertEqual(publish_service_name, "segment_gatherer_iodc_msg")


def suite():
    """Test suite for test_trollduction."""
    loader = unittest.TestLoader()
    mysuite = unittest.TestSuite()
    mysuite.addTest(loader.loadTestsFromTestCase(TestSegmentGatherer))

    return mysuite


if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=2).run(suite())
