#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2018 - 2021 Pytroll developers
#
# Author(s):
#
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

"""Unit testing for segment gatherer."""

import datetime as dt
import logging
import os
import os.path
from unittest.mock import patch, MagicMock, call

import pytest

import posttroll.message
from pytroll_collectors.helper_functions import read_yaml
from pytroll_collectors.segments import SegmentGatherer, ini_to_dict, Status, Message, DO_NOT_COPY_KEYS

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_SINGLE = read_yaml(os.path.join(THIS_DIR, "data/segments_single.yaml"))
CONFIG_SINGLE_NORTH = read_yaml(os.path.join(THIS_DIR, "data/segments_single_north.yaml"))
CONFIG_DOUBLE = read_yaml(os.path.join(THIS_DIR, "data/segments_double.yaml"))
CONFIG_DOUBLE_DIFFERENT = read_yaml(os.path.join(THIS_DIR, "data/segments_double_different.yaml"))
CONFIG_NO_SEG = read_yaml(os.path.join(THIS_DIR,
                                       "data/segments_double_no_segments.yaml"))
CONFIG_COLLECTIONS = read_yaml(os.path.join(THIS_DIR,
                                            "data/segments_double_no_segments_collections.yaml"))
CONFIG_PPS = read_yaml(os.path.join(THIS_DIR, "data/segments_pps.yaml"))
CONFIG_INI = ini_to_dict(os.path.join(THIS_DIR, "data/segments.ini"), "msg")
CONFIG_INI_NO_SEG = ini_to_dict(os.path.join(THIS_DIR, "data/segments.ini"),
                                "goes16")
CONFIG_INI_HIMAWARI = ini_to_dict(os.path.join(THIS_DIR, "data/segments.ini"),
                                  "himawari-8")
CONFIG_NWCSAF_GEO = ini_to_dict(os.path.join(THIS_DIR, "data/segments_nwcsaf_geo.ini"), "nwcsaf_geo")
LOGGING_ERROR = logging.ERROR

fake_config = {
    "patterns": {
        "oak": {
            "pattern": "oak-s{start_time:%Y%m%d%H%M%S}-e{end_time:%Y%m%d%H%M%S}-s{segment}.tree",
            "critical_files": None,
            "wanted_files": ":001-003",
            "all_files": ":001-003",
            "is_critical_set": False,
            "variable_tags": ["start_time", "end_time"]}},
    "timeliness": 10,
    "group_by_minutes": 10,
    "time_name": "start_time"}


class FakeMessage:
    """Fake message."""

    def __init__(self, data, message_type='file', subject='/foo/viirs'):
        """Set up fake message."""
        self.data = data.copy()
        self.type = message_type
        self.subject = subject


class TestSegmentGatherer:
    """Tests for the segment gatherer."""

    def setup_method(self):
        """Set up the testing."""
        self.mda_msg0deg = {"segment": "EPI", "uid": "H-000-MSG3__-MSG3________-_________-EPI______-201611281100-__", "platform_shortname": "MSG3", "start_time": dt.datetime(2016, 11, 28, 11, 0, 0), "nominal_time": dt.datetime(  # noqa
            2016, 11, 28, 11, 0, 0), "uri": "/home/lahtinep/data/satellite/geo/msg/H-000-MSG3__-MSG3________-_________-EPI______-201611281100-__", "platform_name": "Meteosat-10", "channel_name": "", "path": "", "sensor": ["seviri"], "hrit_format": "MSG3"}  # noqa
        self.mda_msg0deg_s3 = {"segment": "EPI", "uid": "H-000-MSG3__-MSG3________-_________-EPI______-201611281100-__", "platform_shortname": "MSG3", "start_time": dt.datetime(2016, 11, 28, 11, 0, 0), "nominal_time": dt.datetime(  # noqa
            2016, 11, 28, 11, 0, 0), "uri": "s3://bucket-name/H-000-MSG3__-MSG3________-_________-EPI______-201611281100-__", "platform_name": "Meteosat-10", "channel_name": "", "path": "", "sensor": ["seviri"], "hrit_format": "MSG3"}  # noqa

        self.mda_msg0deg_south_segment = {"segment": "EPI", "uid": "H-000-MSG3__-MSG3________-VIS006___-000008___-201611281100-__", "platform_shortname": "MSG3", "start_time": dt.datetime(2016, 11, 28, 11, 0, 0), "nominal_time": dt.datetime(  # noqa
            2016, 11, 28, 11, 0, 0), "uri": "/home/lahtinep/data/satellite/geo/msg/H-000-MSG3__-MSG3________-VIS006___-000008___-201611281100-__", "platform_name": "Meteosat-10", "channel_name": "", "path": "", "sensor": ["seviri"], "hrit_format": "MSG3"}  # noqa

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

        self.mda_msg0deg_s3 = {"segment": "EPI", "uid": "H-000-MSG3__-MSG3________-_________-EPI______-201611281100-__", "platform_shortname": "MSG3", "start_time": dt.datetime(2016, 11, 28, 11, 0, 0), "nominal_time": dt.datetime(  # noqa
            2016, 11, 28, 11, 0, 0), "uri": "s3://bucket-name/H-000-MSG3__-MSG3________-_________-EPI______-201611281100-__", "platform_name": "Meteosat-10", "channel_name": "", "path": "", "sensor": ["seviri"], "hrit_format": "MSG3"}  # noqa

        self.mda_msg0deg_file_scheme = {"segment": "EPI", "uid": "H-000-MSG3__-MSG3________-_________-EPI______-201611281100-__", "platform_shortname": "MSG3", "start_time": dt.datetime(2016, 11, 28, 11, 0, 0), "nominal_time": dt.datetime(  # noqa
            2016, 11, 28, 11, 0, 0), "uri": "file:///home/lahtinep/data/satellite/geo/msg/H-000-MSG3__-MSG3________-_________-EPI______-201611281100-__", "platform_name": "Meteosat-10", "channel_name": "", "path": "", "sensor": ["seviri"], "hrit_format": "MSG3"}  # noqa

        self.mda_nwcsaf_geo = {"product": "CRR-Ph", "sensor": "nwc_saf_geo", "platform_name": "Meteosat-11", "start_time": dt.datetime(2023, 2, 14, 13, 0, 0), "uri": "file:///data/S_NWC_CRR-Ph_MSG4_MSG-N-VISIR_20230214T130000Z.nc", "uid": "S_NWC_CRR-Ph_MSG4_MSG-N-VISIR_20230214T130000Z.nc"}  # noqa

        self.msg0deg = SegmentGatherer(CONFIG_SINGLE)
        self.msg0deg_north = SegmentGatherer(CONFIG_SINGLE_NORTH)
        self.msg0deg_iodc = SegmentGatherer(CONFIG_DOUBLE)
        self.hrpt_pps = SegmentGatherer(CONFIG_NO_SEG)
        self.msg_ini = SegmentGatherer(CONFIG_INI)
        self.goes_ini = SegmentGatherer(CONFIG_INI_NO_SEG)
        self.himawari_ini = SegmentGatherer(CONFIG_INI_HIMAWARI)
        self.nwcsaf_geo = SegmentGatherer(CONFIG_NWCSAF_GEO)

    def test_init(self):
        """Test init."""
        assert self.msg0deg._subject is None
        assert list(self.msg0deg._patterns.keys()) == ['msg']
        assert len(self.msg0deg.slots.keys()) == 0
        assert self.msg0deg.time_name == 'start_time'
        assert self.msg0deg._loop is False
        assert self.msg0deg._time_tolerance == 30
        assert self.msg0deg._timeliness.total_seconds() == 10
        assert self.msg0deg._listener is None
        assert self.msg0deg._publisher is None

        # Tests using two filesets start_time_pattern
        assert 'start_time_pattern' in self.msg0deg_iodc._patterns['msg']
        assert '_start_time_pattern' in self.msg0deg_iodc._patterns['msg']
        assert 'start_time_pattern' in self.msg0deg_iodc._patterns['iodc']
        assert '_start_time_pattern' in self.msg0deg_iodc._patterns['iodc']

    def test_init_data(self):
        """Test initializing the data."""
        mda = self.mda_msg0deg.copy()
        fake_message = FakeMessage(mda)
        message = Message(fake_message, self.msg0deg._patterns['msg'])
        self.msg0deg._create_slot(message)

        slot_str = str(mda["start_time"])
        assert list(self.msg0deg.slots.keys())[0] == slot_str
        slot = self.msg0deg.slots[slot_str]
        for key in mda:
            if key not in DO_NOT_COPY_KEYS:
                assert slot.output_metadata[key] == mda[key]
        assert slot['timeout'] is not None
        assert slot['msg']['is_critical_set'] == CONFIG_SINGLE['patterns']['msg']['is_critical_set']
        assert 'critical_files' in slot['msg']
        assert 'wanted_files' in slot['msg']
        assert 'all_files' in slot['msg']
        assert 'received_files' in slot['msg']
        assert 'delayed_files' in slot['msg']
        assert 'missing_files' in slot['msg']

        assert len(slot['msg']['critical_files']) == 2
        assert len(slot['msg']['wanted_files']) == 10
        assert len(slot['msg']['all_files']) == 10

        # Tests using two filesets
        self.msg0deg_iodc._create_slot(message)
        slot = self.msg0deg_iodc.slots[slot_str]
        assert 'collection' in slot.output_metadata
        for key in self.msg0deg_iodc._patterns:
            assert 'dataset' in slot.output_metadata['collection'][key]
            assert 'sensor' in slot.output_metadata['collection'][key]

        # Test using .ini config
        self.msg_ini._create_slot(message)
        slot = self.msg_ini.slots[slot_str]
        assert len(slot['msg']['critical_files']), 2
        assert len(slot['msg']['wanted_files']), 38
        assert len(slot['msg']['all_files']), 114

    def test_compose_filenames_explicit_segments(self):
        """Test composing the filenames when segment names don't need expansion."""
        mda = self.mda_msg0deg.copy()
        segment_list = self.msg0deg._patterns['msg']['critical_files']
        fname_set = self._get_filenames(self.msg0deg, mda, 'msg', segment_list)
        assert isinstance(fname_set, set)
        assert len(fname_set) == 2
        assert "H-000-MSG3__-MSG3________-_________-PRO______-201611281100-__" in fname_set
        assert "H-000-MSG3__-MSG3________-_________-EPI______-201611281100-__" in fname_set

    @staticmethod
    def _get_filenames(segment_gatherer, mda, section, segment_list):
        fake_message = FakeMessage(mda)
        message = Message(fake_message, segment_gatherer._patterns[section])
        segment_gatherer._create_slot(message)
        parser = segment_gatherer._patterns[section].parser
        slot_str = str(mda["start_time"])
        slot = segment_gatherer.slots[slot_str]
        return slot.compose_filenames(parser, segment_list)

    def test_compose_filenames_none(self):
        """Test composing the filenames when no segments are defined in the config."""
        mda = self.mda_msg0deg.copy()
        fname_set = self._get_filenames(self.msg0deg, mda, 'msg', None)
        assert len(fname_set) == 0

    def test_compose_filenames_range_of_segments(self):
        """Test that the segments can be defined with numeric ranges."""
        mda = self.mda_msg0deg.copy()
        segment_list = self.msg0deg._patterns['msg']['wanted_files']
        fname_set = self._get_filenames(self.msg0deg, mda, 'msg', segment_list)
        assert len(fname_set) == 10

    def test_compose_filenames_no_segments_undefined_variable_tag(self):
        """Test using filesets with no segments and a single un-typed variable tag."""
        mda = self.mda_hrpt.copy()
        segment_list = self.hrpt_pps._patterns['hrpt']['critical_files']
        fname_set = self._get_filenames(self.hrpt_pps, mda, 'hrpt', segment_list)
        assert len(fname_set) == 1
        assert "hrpt_*_20180319_0955_28538.l1b" in fname_set

    def test_compose_filenames_no_segments_typed_variable_tags(self):
        """Test using filesets with no segments and typed variable tags."""
        mda = self.mda_pps.copy()
        segment_list = self.hrpt_pps._patterns['pps']['critical_files']
        fname_set = self._get_filenames(self.hrpt_pps, mda, 'pps', segment_list)
        assert len(fname_set) == 1
        assert "S_NWC_CMA_*_28538_20180319T0955???Z_????????T???????Z.nc" in fname_set

    def test_compose_filenames_no_segments_ini_config(self):
        """Test using filesets with no segments, INI config."""
        mda = self.mda_goes16.copy()
        segment_list = self.goes_ini._patterns['goes16']['critical_files']
        fname_set = self._get_filenames(self.goes_ini, mda, 'goes16', segment_list)
        assert len(fname_set) == 0

    def test_compose_filenames_dash_in_segment_name(self):
        """Test a segment name has a dash in it (CRR-Ph)."""
        mda = self.mda_nwcsaf_geo.copy()
        segment_list = self.nwcsaf_geo._patterns['nwcsaf_geo']['critical_files']
        fname_set = self._get_filenames(self.nwcsaf_geo, mda, 'nwcsaf_geo', segment_list)
        assert "S_NWC_CRR-Ph_MSG4_MSG-N-VISIR_20230214T130000Z.nc" in fname_set

    def test_update_timeout(self):
        """Test updating the timeout."""
        import numpy as np

        mda = self.mda_msg0deg.copy()
        slot_str = str(mda["start_time"])
        fake_message = FakeMessage(mda)
        message = Message(fake_message, self.msg0deg._patterns['msg'])
        self.msg0deg._create_slot(message)
        now = dt.datetime.utcnow()
        slot = self.msg0deg.slots[slot_str]
        slot.update_timeout()
        diff = slot['timeout'] - now
        np.testing.assert_almost_equal(diff.total_seconds(), 10.0, decimal=3)

    def test_slot_is_ready(self):
        """Test if a slot is ready."""
        mda = self.mda_msg0deg.copy()
        slot_str = str(mda["start_time"])
        fake_message = FakeMessage(mda)
        message = Message(fake_message, self.msg0deg._patterns['msg'])
        self.msg0deg._create_slot(message)
        slot = self.msg0deg.slots[slot_str]
        func = slot.get_status

        res = func()
        assert res == Status.SLOT_NOT_READY

        slot['msg']['files_till_premature_publish'] = 8

        slot['msg']['received_files'] |= set(['H-000-MSG3__-MSG3________-_________-PRO______-201611281100-__'])
        res = func()
        assert res == Status.SLOT_NOT_READY

        slot['msg']['received_files'] |= set(['H-000-MSG3__-MSG3________-_________-EPI______-201611281100-__'])
        res = func()
        assert res == Status.SLOT_NOT_READY

        slot['msg']['is_critical_set'] = False
        slot['msg']['received_files'] |= set(['H-000-MSG3__-MSG3________-VIS006___-000001___-201611281100-__',
                                              'H-000-MSG3__-MSG3________-VIS006___-000002___-201611281100-__',
                                              'H-000-MSG3__-MSG3________-VIS006___-000003___-201611281100-__',
                                              'H-000-MSG3__-MSG3________-VIS006___-000004___-201611281100-__',
                                              'H-000-MSG3__-MSG3________-VIS006___-000005___-201611281100-__'])
        res = func()
        assert res == Status.SLOT_NONCRITICAL_NOT_READY

        slot['msg']['received_files'] |= set(['H-000-MSG3__-MSG3________-VIS006___-000006___-201611281100-__'])
        res = func()
        assert res == Status.SLOT_READY_BUT_WAIT_FOR_MORE

        slot['msg']['received_files'] |= set(['H-000-MSG3__-MSG3________-VIS006___-000007___-201611281100-__',
                                              'H-000-MSG3__-MSG3________-VIS006___-000008___-201611281100-__',
                                              'H-000-MSG3__-MSG3________-_________-EPI______-201611281100-__',
                                              'H-000-MSG3__-MSG3________-_________-PRO______-201611281100-__'])
        res = func()
        assert res == Status.SLOT_READY

    def test_get_collection_status(self):
        """Test getting the collection status."""
        mda = self.mda_msg0deg.copy()
        slot_str = str(mda["start_time"])

        now = dt.datetime.utcnow()
        future = now + dt.timedelta(minutes=1)
        past = now - dt.timedelta(minutes=1)

        fake_message = FakeMessage(mda)
        message = Message(fake_message, self.msg0deg._patterns['msg'])
        self.msg0deg._create_slot(message)
        slot = self.msg0deg.slots[slot_str]
        func = slot.get_collection_status

        status = {}
        assert func(status, future) == Status.SLOT_NOT_READY

        status = {'foo': Status.SLOT_NOT_READY}
        assert func(status, past) == Status.SLOT_OBSOLETE_TIMEOUT
        assert func(status, future) == Status.SLOT_NOT_READY

        status = {'foo': Status.SLOT_NONCRITICAL_NOT_READY}

        slot['foo'] = {'received_files': [0, 1]}
        assert func(status, past) == Status.SLOT_READY
        assert func(status, future) == Status.SLOT_NONCRITICAL_NOT_READY

        status = {'foo': Status.SLOT_READY}
        assert func(status, past) == Status.SLOT_READY
        assert func(status, future) == Status.SLOT_READY

        status = {'foo': Status.SLOT_READY_BUT_WAIT_FOR_MORE}
        assert func(status, past) == Status.SLOT_READY
        assert func(status, future) == Status.SLOT_READY_BUT_WAIT_FOR_MORE

        # More than one fileset

        status = {'foo': Status.SLOT_NOT_READY, 'bar': Status.SLOT_NOT_READY}
        assert func(status, past) == Status.SLOT_OBSOLETE_TIMEOUT
        assert func(status, future) == Status.SLOT_NOT_READY

        status = {'foo': Status.SLOT_NOT_READY, 'bar': Status.SLOT_NONCRITICAL_NOT_READY}
        assert func(status, past) == Status.SLOT_OBSOLETE_TIMEOUT
        assert func(status, future) == Status.SLOT_NOT_READY

        status = {'foo': Status.SLOT_NOT_READY, 'bar': Status.SLOT_READY}
        assert func(status, past) == Status.SLOT_OBSOLETE_TIMEOUT
        assert func(status, future) == Status.SLOT_NOT_READY

        status = {'foo': Status.SLOT_NOT_READY, 'bar': Status.SLOT_READY_BUT_WAIT_FOR_MORE}
        assert func(status, past) == Status.SLOT_OBSOLETE_TIMEOUT
        assert func(status, future) == Status.SLOT_NOT_READY

        status = {'foo': Status.SLOT_NONCRITICAL_NOT_READY,
                  'bar': Status.SLOT_NONCRITICAL_NOT_READY}
        assert func(status, past) == Status.SLOT_READY
        assert func(status, future) == Status.SLOT_NONCRITICAL_NOT_READY

        status = {'foo': Status.SLOT_NONCRITICAL_NOT_READY, 'bar': Status.SLOT_READY}
        assert func(status, past) == Status.SLOT_READY
        assert func(status, future) == Status.SLOT_NONCRITICAL_NOT_READY

        status = {'foo': Status.SLOT_NONCRITICAL_NOT_READY,
                  'bar': Status.SLOT_READY_BUT_WAIT_FOR_MORE}
        assert func(status, past), Status.SLOT_READY
        assert func(status, future) == Status.SLOT_NONCRITICAL_NOT_READY

        status = {'foo': Status.SLOT_READY, 'bar': Status.SLOT_READY}
        assert func(status, past), Status.SLOT_READY
        assert func(status, future), Status.SLOT_READY

        status = {'foo': Status.SLOT_READY, 'bar': Status.SLOT_READY_BUT_WAIT_FOR_MORE}
        assert func(status, past), Status.SLOT_READY
        assert func(status, future) == Status.SLOT_READY_BUT_WAIT_FOR_MORE

        status = {'foo': Status.SLOT_READY_BUT_WAIT_FOR_MORE,
                  'bar': Status.SLOT_READY_BUT_WAIT_FOR_MORE}
        assert func(status, past), Status.SLOT_READY
        assert func(status, future) == Status.SLOT_READY_BUT_WAIT_FOR_MORE

    def test_process_message_without_uid(self, caplog):
        """Test adding a file."""
        # Single fileset
        mda = self.mda_msg0deg.copy()
        del mda['uid']
        msg = FakeMessage(mda)
        col = self.msg0deg
        with caplog.at_level(logging.DEBUG):
            col.process(msg)
            logs = [rec.message for rec in caplog.records]
            assert "No key 'uid' in message." in logs

    def test_process_one_message_before_init(self, caplog):
        """Test adding a file."""
        mda = self.mda_msg0deg.copy()
        msg = FakeMessage(mda)
        col = self.msg0deg
        with caplog.at_level(logging.DEBUG):
            col.process(msg)
            logs = [rec.message for rec in caplog.records]
            assert "Adding new slot:" in logs[0]

    def test_process_one_message_outside_range_of_interest(self, caplog):
        """Test processing a file outside the range of interest."""
        mda = self.mda_msg0deg_south_segment.copy()
        msg = FakeMessage(mda)
        col = self.msg0deg_north
        with caplog.at_level(logging.DEBUG):
            col.process(msg)
            logs = [rec.message for rec in caplog.records]
            assert "H-000-MSG3__-MSG3________-VIS006___-000008___-201611281100-__ not in " in logs[2]

    def test_process_message_twice(self, caplog):
        """Test processing a message."""
        mda = self.mda_msg0deg.copy()
        msg = FakeMessage(mda)
        col = self.msg0deg
        message = Message(msg, col._patterns['msg'])
        col._create_slot(message)
        col.process(msg)
        with caplog.at_level(logging.DEBUG):
            col.process(msg)
            logs = [rec.message for rec in caplog.records]
            assert 'File already received' in logs

    def test_process_message_unknown_file(self, caplog):
        """Test processing a message with an unknown file uid."""
        mda = self.mda_msg0deg.copy()
        mda['uid'] = "blablabla"
        msg = FakeMessage(mda)
        col = self.msg0deg
        with caplog.at_level(logging.DEBUG):
            col.process(msg)
            logs = [rec.message for rec in caplog.records]
            assert 'No parser matching message, skipping.' in logs

    def test_process_message_force_local_files(self):
        """Test processing message with scheme when config says files are local."""
        mda = self.mda_msg0deg.copy()
        expected_uri = mda['uri']
        mda['uri'] = 'SCHEME://host.foo.bar' + mda['uri']
        msg = FakeMessage(mda)
        col = SegmentGatherer(CONFIG_SINGLE)
        col._config['all_files_are_local'] = True
        col.process(msg)
        slot = col.slots[str(mda['start_time'])]
        uri = slot.output_metadata['dataset'][0]['uri']
        assert uri == expected_uri

    def test_process_message_force_local_files_check_existing(self):
        """Test processing message with scheme when config says files are local and existing files are checked."""
        mda = self.mda_msg0deg.copy()
        mda['uri'] = 'SCHEME://host.foo.bar' + mda['uri']
        msg = FakeMessage(mda)
        col = SegmentGatherer(CONFIG_SINGLE)
        # Without forcing all files to be local the processing would fail with
        # "ValueError: Protocol not known: scheme"
        col._config['all_files_are_local'] = True
        col._config['check_existing_files_after_start'] = True
        col.process(msg)

    def test_add_single_file(self):
        """Test adding a file."""
        msg = FakeMessage(self.mda_msg0deg)
        col = self.msg0deg
        message = Message(msg, col._patterns['msg'])
        col._create_slot(message)
        time_slot = list(col.slots.keys())[0]
        key = list(CONFIG_SINGLE['patterns'].keys())[0]
        slot = col.slots[time_slot]
        res = slot.add_file(message)
        assert res is None
        assert len(col.slots[time_slot][key]['received_files']) == 1
        meta = col.slots[time_slot].output_metadata
        assert len(meta['dataset']) == 1
        assert 'uri' in meta['dataset'][0]
        assert 'uid' in meta['dataset'][0]
        assert meta['dataset'][0]['uri'].startswith('/home/lahtinep/data/satellite/geo/msg/')

    def test_add_single_file_s3_scheme(self):
        """Test adding a file that is in S3 storage."""
        msg = FakeMessage(self.mda_msg0deg_s3)
        col = self.msg0deg
        message = Message(msg, col._patterns['msg'])
        col._create_slot(message)
        time_slot = list(col.slots.keys())[0]
        slot = col.slots[time_slot]
        _ = slot.add_file(message)
        meta = col.slots[time_slot].output_metadata
        assert meta['dataset'][0]['uri'].startswith('s3://bucket-name/')

    def test_add_single_file_file_scheme(self):
        """Test adding a file that has file:// scheme in the URI."""
        msg = FakeMessage(self.mda_msg0deg_file_scheme)
        col = self.msg0deg
        message = Message(msg, col._patterns['msg'])
        col._create_slot(message)
        time_slot = list(col.slots.keys())[0]
        slot = col.slots[time_slot]
        _ = slot.add_file(message)
        meta = col.slots[time_slot].output_metadata
        assert meta['dataset'][0]['uri'].startswith('file:///home/lahtinep/data/satellite/geo/msg/')

    def test_add_single_s3_file(self):
        """Test adding a file that is in S3 storage."""
        msg = FakeMessage(self.mda_msg0deg_s3)
        col = self.msg0deg
        message = Message(msg, col._patterns['msg'])
        col._create_slot(message)
        time_slot = list(col.slots.keys())[0]
        slot = col.slots[time_slot]
        _ = slot.add_file(message)
        meta = col.slots[time_slot].output_metadata
        assert meta['dataset'][0]['uri'] == msg.data['uri']

    def test_add_two_files(self):
        """Test adding two files."""
        msg_data = {'msg': self.mda_msg0deg.copy(), 'iodc': self.mda_iodc.copy()}
        col = self.msg0deg_iodc
        fake_message = FakeMessage(self.mda_msg0deg.copy())
        message = Message(fake_message, col._patterns['msg'])
        col._create_slot(message)
        time_slot = str(msg_data['msg']['start_time'])
        i = 0
        for key in CONFIG_DOUBLE['patterns']:
            message = Message(FakeMessage(msg_data[key]), col._patterns[key])
            slot = col.slots[time_slot]
            res = slot.add_file(message)
            assert res is None
            assert len(col.slots[time_slot][key]['received_files']) == 1
            meta = col.slots[time_slot].output_metadata
            assert len(meta['collection'][key]['dataset']) == 1
            assert 'uri' in meta['collection'][key]['dataset'][0]
            assert 'uid' in meta['collection'][key]['dataset'][0]
            i += 1

    def test_add_two_files_without_segments(self):
        """Test adding two files without segments."""
        msg_data = {'hrpt': self.mda_hrpt.copy(),
                    'pps': self.mda_pps.copy()}
        col = self.hrpt_pps
        fake_message = FakeMessage(self.mda_hrpt.copy())
        message = Message(fake_message, col._patterns['hrpt'])
        col._create_slot(message)
        time_slot = str(msg_data['hrpt']['start_time'])
        i = 0
        for key in CONFIG_NO_SEG['patterns']:
            message = Message(FakeMessage(msg_data[key]), col._patterns[key])
            slot = col.slots[time_slot]
            res = slot.add_file(message)
            assert res is None
            assert len(col.slots[time_slot][key]['received_files']) == 1
            meta = col.slots[time_slot].output_metadata
            assert len(meta['collection'][key]['dataset']) == 1
            i += 1

    def test_ini_to_dict(self):
        """Test ini conversion to dict."""
        config = ini_to_dict(os.path.join(THIS_DIR, "data/segments.ini"), "msg")
        assert 'patterns' in config
        assert 'posttroll' in config
        assert 'time_tolerance' in config
        assert 'timeliness' in config
        assert 'time_name' in config
        assert 'check_existing_files_after_start' in config

        assert 'topics' in config['posttroll']
        assert 'nameservers' in config['posttroll']
        assert 'addresses' in config['posttroll']
        assert 'topics' in config['posttroll']
        assert 'publish_port' in config['posttroll']
        assert 'publish_topic' in config['posttroll']

        assert 'msg' in config['patterns']
        assert 'pattern' in config['patterns']['msg']
        assert 'critical_files' in config['patterns']['msg']
        assert 'wanted_files' in config['patterns']['msg']
        assert 'all_files' in config['patterns']['msg']
        assert 'is_critical_set' in config['patterns']['msg']
        assert 'variable_tags' in config['patterns']['msg']

    def test_check_schedule_time(self):
        """Test Check Schedule Time."""
        hour = self.msg0deg_iodc._patterns['msg']['_start_time_pattern']
        assert self.msg0deg.check_if_time_is_in_interval(hour, dt.time(9, 0))
        assert not self.msg0deg.check_if_time_is_in_interval(hour, dt.time(9, 30))
        assert not self.msg0deg.check_if_time_is_in_interval(hour, dt.time(23, 0))
        hour = self.msg0deg_iodc._patterns['iodc']['_start_time_pattern']
        assert self.msg0deg.check_if_time_is_in_interval(hour, dt.time(4, 15))
        assert not self.msg0deg.check_if_time_is_in_interval(hour, dt.time(4, 30))
        assert not self.msg0deg.check_if_time_is_in_interval(hour, dt.time(11, 0))

    def test_copy_metadata(self):
        """Test combining metadata from a message and parsed from filename."""
        from pytroll_collectors.segments import filter_metadata

        mda = {'a': 1, 'b': 2}
        msg = MagicMock()
        msg.data = {'a': 2, 'c': 3}

        res = filter_metadata(mda, msg.data)
        assert res['a'] == 2
        assert res['b'] == 2
        assert res['c'] == 3

        # Keep 'a' from parsed metadata
        res = filter_metadata(mda, msg.data, keep_parsed_keys=['a'])
        assert res['a'] == 1
        assert res['b'] == 2
        assert res['c'] == 3

        # Keep 'a' from parsed metadata configured for one of more patterns
        res = filter_metadata(mda, msg.data, local_keep_parsed_keys=['a'])
        assert res['a'] == 1
        assert res['b'] == 2
        assert res['c'] == 3

    def test_publish_service_name(self):
        """Test publish service name.

        Need to be equal each time.
        """
        col = self.msg0deg_iodc
        publish_service_name = col._generate_publish_service_name()
        assert publish_service_name == "segment_gatherer_iodc_msg"

    @patch("pytroll_collectors.segments.logging")
    @patch("fsspec.filesystem")
    def test_check_and_add_existing_files_first_message(self, filesystem, logging):
        """Test that existing matching files are added to the slot when the first message is received."""
        existing_files = [
            "/home/lahtinep/data/satellite/geo/msg/H-000-MSG3__-MSG3________-VIS006___-000007___-201611281100-__",
            "/home/lahtinep/data/satellite/geo/msg/H-000-MSG3__-MSG3________-VIS006___-000008___-201611281100-__",
            # A file that should not match
            "/home/lahtinep/data/satellite/geo/msg/H-000-MSG4__-MSG4________-VIS006___-000008___-201611281100-__"]
        filesystem.return_value = _get_filesystem_return_value(existing_files)
        message = _get_message_from_metadata_and_patterns(self.mda_msg0deg, self.msg0deg._patterns['msg'])
        slot = _create_and_get_slot(self.msg0deg, message)
        self.msg0deg._config['check_existing_files_after_start'] = True

        self.msg0deg.check_and_add_existing_files(slot, message)
        assert call(logging.DEBUG) in logging.disable.mock_calls
        assert call(logging.NOTSET) in logging.disable.mock_calls
        for fname in existing_files[:-1]:
            assert os.path.basename(fname) in slot._info['msg']['received_files']
        assert os.path.basename(existing_files[-1]) not in slot._info['msg']['received_files']

    @patch("pytroll_collectors.segments.logging")
    @patch("fsspec.filesystem")
    def test_check_and_add_existing_files_not_first_message(self, filesystem, logging):
        """Test that existing files are not checked/added after the first message."""
        existing_files = [
            "/home/lahtinep/data/satellite/geo/msg/H-000-MSG3__-MSG3________-VIS006___-000005___-201611281100-__",
            "/home/lahtinep/data/satellite/geo/msg/H-000-MSG3__-MSG3________-VIS006___-000006___-201611281100-__"]
        filesystem.return_value = _get_filesystem_return_value(existing_files)
        message = _get_message_from_metadata_and_patterns(self.mda_msg0deg, self.msg0deg._patterns['msg'])
        slot = _create_and_get_slot(self.msg0deg, message)
        self.msg0deg._config['check_existing_files_after_start'] = True

        self.msg0deg.check_and_add_existing_files(slot, message)
        # Clear the received files and rerun, now the files should not be added
        slot._info['msg']['received_files'] = {}
        self.msg0deg.check_and_add_existing_files(slot, message)
        for fname in existing_files[:-1]:
            assert os.path.basename(fname) not in slot._info['msg']['received_files']

    @patch("pytroll_collectors.segments.logging")
    @patch("fsspec.filesystem")
    def test_check_and_add_existing_files_ignore_collection_messages(self, filesystem, logging, caplog):
        """Test that messages with other types than 'file' are ignored."""
        message = _get_message_from_metadata_and_patterns(self.mda_msg0deg, self.msg0deg._patterns['msg'])
        slot = _create_and_get_slot(self.msg0deg, message)
        self.msg0deg._config['check_existing_files_after_start'] = True
        message.type = "collection"
        self.msg0deg._is_first_message_after_start = True

        with caplog.at_level(LOGGING_ERROR):
            self.msg0deg.check_and_add_existing_files(slot, message)
            logs = [rec.message for rec in caplog.records]
            assert "Only 'file' messages are supported." in logs

    @patch("pytroll_collectors.segments.logging")
    @patch("fsspec.filesystem")
    def test_check_and_add_existing_s3_files(self, filesystem, logging):
        """Test that existing matching files are added to the slot in S3 storage."""
        # There's no 's3://' in the glob result of fsspec.filesystem("s3").glob() call
        existing_files = [
            "bucket-name/H-000-MSG3__-MSG3________-VIS006___-000007___-201611281100-__",
            "bucket-name/H-000-MSG3__-MSG3________-VIS006___-000008___-201611281100-__",
            # A file that should not match
            "bucket-name/H-000-MSG4__-MSG4________-VIS006___-000008___-201611281100-__"]
        filesystem.return_value = _get_filesystem_return_value(existing_files)
        message = _get_message_from_metadata_and_patterns(self.mda_msg0deg_s3, self.msg0deg._patterns['msg'])
        slot = _create_and_get_slot(self.msg0deg, message)
        self.msg0deg._config['check_existing_files_after_start'] = True

        self.msg0deg.check_and_add_existing_files(slot, message)
        mask_call = 's3://bucket-name/H-000-????__-????________-?????????-?????????-????????????-__'
        filesystem.return_value.glob.assert_called_with(mask_call)
        for fname in existing_files[:-1]:
            assert os.path.basename(fname) in slot._info['msg']['received_files']
        assert os.path.basename(existing_files[-1]) not in slot._info['msg']['received_files']
        for dset in slot.output_metadata['dataset']:
            assert dset['uri'].startswith('s3://')

    def test_messaging(self):
        """Test that messaging is initialized correctly."""
        with patch('pytroll_collectors.utils.create_publisher_from_dict_config') as creator:
            with patch('pytroll_collectors.segments.ListenerContainer') as ListenerContainer:
                self.msg_ini._setup_messaging()
        assert_messaging('segment_gatherer_msg', 0, None, None, creator, ListenerContainer)

    def test_messaging_disable_nameservers(self):
        """Test that messaging is initialized correctly when nameserver connections are disabled."""
        with patch('pytroll_collectors.utils.create_publisher_from_dict_config') as creator:
            with patch('pytroll_collectors.segments.ListenerContainer') as ListenerContainer:
                self.goes_ini._setup_messaging()
        assert_messaging('segment_gatherer_goes16', '12345', False, False, creator, ListenerContainer)

    def test_messaging_multiple_nameservers(self):
        """Test that messaging is initialized correctly when nameserver connections are disabled."""
        with patch('pytroll_collectors.utils.create_publisher_from_dict_config') as creator:
            with patch('pytroll_collectors.segments.ListenerContainer') as ListenerContainer:
                self.himawari_ini._setup_messaging()
        assert_messaging('segment_gatherer_himawari-8', 0, ['localhost', 'otherserver'],
                         'localhost', creator, ListenerContainer)

    def test_publisher_disable_nameservers_multiple_nameservers(self):
        """Test that messaging is initialized correctly when nameserver connections are disabled."""
        with patch('pytroll_collectors.utils.create_publisher_from_dict_config') as creator:
            self.msg0deg._setup_publisher()
        assert_messaging('segment_gatherer_msg', 0, False, None, creator, None)

    def test_listener_use_first_nameserver(self):
        """Test that listener usest the first nameserver when multiple are given."""
        with patch('pytroll_collectors.segments.ListenerContainer') as ListenerContainer:
            self.msg0deg._setup_listener()
        assert_messaging(None, None, None, 'localhost', None, ListenerContainer)


def _get_message_from_metadata_and_patterns(mda, patterns):
    fake_message = FakeMessage(mda)
    return Message(fake_message, patterns)


def _get_filesystem_return_value(existing_files):
    glob = MagicMock()
    glob.return_value = existing_files
    fs_ = MagicMock()
    fs_.glob = glob
    return fs_


def _create_and_get_slot(gatherer, message):
    gatherer._create_slot(message)
    slot_str = str(message.metadata["start_time"])
    return gatherer.slots[slot_str]


def assert_messaging(name, port, publisher_nameservers, listener_nameserver, creator, listener_container):
    """Test messaging settings."""
    if creator:
        _assert_creator(name, port, publisher_nameservers, creator)
    if listener_container:
        _assert_listener_container(listener_container, listener_nameserver)


def _assert_creator(name, port, publisher_nameservers, creator):
    expected_publisher = {
        'name': name,
        'port': port,
        'nameservers': publisher_nameservers,
    }
    creator.assert_called_with(expected_publisher)
    creator.return_value.start.assert_called_once()


def _assert_listener_container(listener_container, listener_nameserver):
    listener_container.assert_called_once_with(
        topics=['/foo/bar'],
        addresses=None,
        nameserver=listener_nameserver,
        services='',
    )


viirs_message = (
    'pytroll://foo/viirs/segment/SDR/1B/polar/direct_readout dataset safusr.u@lxserv1043.smhi.se '
    '2020-10-13T05:33:06.568191 v1.01 application/json {"start_time": "2020-10-13T05:17:21.200000", "end_time": '
    '"2020-10-13T05:18:43", "orbit_number": 15037, "platform_name": "NOAA-20", "sensor": "viirs", "format": '
    '"SDR", "type": "HDF5", "data_processing_level": "1B", "variant": "DR", "orig_orbit_number": 15036, "dataset": '
    '[{"uri": "ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/GMODO_j01_d20201013_t0517210_e0518437_b15037_c20201013052249968858_cspp_dev.h5", '  # noqa
    '"uid": "GMODO_j01_d20201013_t0517210_e0518437_b15037_c20201013052249968858_cspp_dev.h5"}, {"uri": '
    '"ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/GMTCO_j01_d20201013_t0517210_e0518437_b15037_c20201013052249869932_cspp_dev.h5", '  # noqa
    '"uid": "GMTCO_j01_d20201013_t0517210_e0518437_b15037_c20201013052249869932_cspp_dev.h5"}, {"uri": '
    '"ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVM01_j01_d20201013_t0517210_e0518437_b15037_c20201013052306592492_cspp_dev.h5", '  # noqa
    '"uid": "SVM01_j01_d20201013_t0517210_e0518437_b15037_c20201013052306592492_cspp_dev.h5"}, {"uri": '
    '"ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVM02_j01_d20201013_t0517210_e0518437_b15037_c20201013052306625314_cspp_dev.h5", '  # noqa
    '"uid": "SVM02_j01_d20201013_t0517210_e0518437_b15037_c20201013052306625314_cspp_dev.h5"}, {"uri": '
    '"ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVM03_j01_d20201013_t0517210_e0518437_b15037_c20201013052306658107_cspp_dev.h5", '  # noqa
    '"uid": "SVM03_j01_d20201013_t0517210_e0518437_b15037_c20201013052306658107_cspp_dev.h5"}, {"uri": '
    '"ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVM04_j01_d20201013_t0517210_e0518437_b15037_c20201013052306695328_cspp_dev.h5", '  # noqa
    '"uid": "SVM04_j01_d20201013_t0517210_e0518437_b15037_c20201013052306695328_cspp_dev.h5"}, {"uri": '
    '"ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVM05_j01_d20201013_t0517210_e0518437_b15037_c20201013052306731912_cspp_dev.h5", '  # noqa
    '"uid": "SVM05_j01_d20201013_t0517210_e0518437_b15037_c20201013052306731912_cspp_dev.h5"}, {"uri": '
    '"ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVM06_j01_d20201013_t0517210_e0518437_b15037_c20201013052306769387_cspp_dev.h5", '  # noqa
    '"uid": "SVM06_j01_d20201013_t0517210_e0518437_b15037_c20201013052306769387_cspp_dev.h5"}, {"uri": '
    '"ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVM07_j01_d20201013_t0517210_e0518437_b15037_c20201013052307952528_cspp_dev.h5", '  # noqa
    '"uid": "SVM07_j01_d20201013_t0517210_e0518437_b15037_c20201013052307952528_cspp_dev.h5"}, {"uri": '
    '"ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVM08_j01_d20201013_t0517210_e0518437_b15037_c20201013052306840611_cspp_dev.h5", '  # noqa
    '"uid": "SVM08_j01_d20201013_t0517210_e0518437_b15037_c20201013052306840611_cspp_dev.h5"}, {"uri": '
    '"ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVM09_j01_d20201013_t0517210_e0518437_b15037_c20201013052306876165_cspp_dev.h5", '  # noqa
    '"uid": "SVM09_j01_d20201013_t0517210_e0518437_b15037_c20201013052306876165_cspp_dev.h5"}, {"uri": '
    '"ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVM10_j01_d20201013_t0517210_e0518437_b15037_c20201013052306912966_cspp_dev.h5", '  # noqa
    '"uid": "SVM10_j01_d20201013_t0517210_e0518437_b15037_c20201013052306912966_cspp_dev.h5"}, {"uri": '
    '"ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVM11_j01_d20201013_t0517210_e0518437_b15037_c20201013052306946712_cspp_dev.h5", '  # noqa
    '"uid": "SVM11_j01_d20201013_t0517210_e0518437_b15037_c20201013052306946712_cspp_dev.h5"}, {"uri": '
    '"ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVM12_j01_d20201013_t0517210_e0518437_b15037_c20201013052306979943_cspp_dev.h5", '  # noqa
    '"uid": "SVM12_j01_d20201013_t0517210_e0518437_b15037_c20201013052306979943_cspp_dev.h5"}, {"uri": '
    '"ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVM13_j01_d20201013_t0517210_e0518437_b15037_c20201013052307008426_cspp_dev.h5", '  # noqa
    '"uid": "SVM13_j01_d20201013_t0517210_e0518437_b15037_c20201013052307008426_cspp_dev.h5"}, {"uri": '
    '"ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVM14_j01_d20201013_t0517210_e0518437_b15037_c20201013052307049977_cspp_dev.h5", '  # noqa
    '"uid": "SVM14_j01_d20201013_t0517210_e0518437_b15037_c20201013052307049977_cspp_dev.h5"}, {"uri": '
    '"ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVM15_j01_d20201013_t0517210_e0518437_b15037_c20201013052307083732_cspp_dev.h5", '  # noqa
    '"uid": "SVM15_j01_d20201013_t0517210_e0518437_b15037_c20201013052307083732_cspp_dev.h5"}, {"uri": '
    '"ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVM16_j01_d20201013_t0517210_e0518437_b15037_c20201013052307116885_cspp_dev.h5", '  # noqa
    '"uid": "SVM16_j01_d20201013_t0517210_e0518437_b15037_c20201013052307116885_cspp_dev.h5"}, {"uri": '
    '"ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/GIMGO_j01_d20201013_t0517210_e0518437_b15037_c20201013052249453008_cspp_dev.h5", '  # noqa
    '"uid": "GIMGO_j01_d20201013_t0517210_e0518437_b15037_c20201013052249453008_cspp_dev.h5"}, {"uri": '
    '"ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/GITCO_j01_d20201013_t0517210_e0518437_b15037_c20201013052249043730_cspp_dev.h5", '  # noqa
    '"uid": "GITCO_j01_d20201013_t0517210_e0518437_b15037_c20201013052249043730_cspp_dev.h5"}, {"uri": '
    '"ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVI01_j01_d20201013_t0517210_e0518437_b15037_c20201013052306211501_cspp_dev.h5", '  # noqa
    '"uid": "SVI01_j01_d20201013_t0517210_e0518437_b15037_c20201013052306211501_cspp_dev.h5"}, {"uri": '
    '"ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVI02_j01_d20201013_t0517210_e0518437_b15037_c20201013052306288882_cspp_dev.h5", '  # noqa
    '"uid": "SVI02_j01_d20201013_t0517210_e0518437_b15037_c20201013052306288882_cspp_dev.h5"}, {"uri": '
    '"ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVI03_j01_d20201013_t0517210_e0518437_b15037_c20201013052306364990_cspp_dev.h5", '  # noqa
    '"uid": "SVI03_j01_d20201013_t0517210_e0518437_b15037_c20201013052306364990_cspp_dev.h5"}, {"uri": '
    '"ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVI04_j01_d20201013_t0517210_e0518437_b15037_c20201013052306440875_cspp_dev.h5", '  # noqa
    '"uid": "SVI04_j01_d20201013_t0517210_e0518437_b15037_c20201013052306440875_cspp_dev.h5"}, {"uri": '
    '"ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVI05_j01_d20201013_t0517210_e0518437_b15037_c20201013052306516433_cspp_dev.h5", '  # noqa
    '"uid": "SVI05_j01_d20201013_t0517210_e0518437_b15037_c20201013052306516433_cspp_dev.h5"}, {"uri": '
    '"ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/GDNBO_j01_d20201013_t0517210_e0518437_b15037_c20201013052248852780_cspp_dev.h5", '  # noqa
    '"uid": "GDNBO_j01_d20201013_t0517210_e0518437_b15037_c20201013052248852780_cspp_dev.h5"}, {"uri": '
    '"ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVDNB_j01_d20201013_t0517210_e0518437_b15037_c20201013052306011816_cspp_dev.h5", '  # noqa
    '"uid": "SVDNB_j01_d20201013_t0517210_e0518437_b15037_c20201013052306011816_cspp_dev.h5"}, {"uri": '
    '"ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/IVCDB_j01_d20201013_t0517210_e0518437_b15037_c20201013052306046518_cspu_pop.h5", '  # noqa
    '"uid": "IVCDB_j01_d20201013_t0517210_e0518437_b15037_c20201013052306046518_cspu_pop.h5"}]}')


viirs_message_data = {'start_time': dt.datetime(2020, 10, 13, 5, 17, 21),
                      'end_time': dt.datetime(2020, 10, 13, 5, 18, 43),
                      'orbit_number': 15037,
                      'platform_name': 'NOAA-20',
                      'sensor': 'viirs',
                      'format': 'SDR',
                      'type': 'HDF5',
                      'data_processing_level': '1B',
                      'variant': 'DR',
                      'orig_orbit_number': 15036,
                      'dataset': [{
                                      'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/GMODO_j01_d20201013_t0517210_e0518437_b15037_c20201013052249968858_cspp_dev.h5',  # noqa
                                      'uid': 'GMODO_j01_d20201013_t0517210_e0518437_b15037_c20201013052249968858_cspp_dev.h5'},  # noqa
                                  {
                                      'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/GMTCO_j01_d20201013_t0517210_e0518437_b15037_c20201013052249869932_cspp_dev.h5',  # noqa
                                      'uid': 'GMTCO_j01_d20201013_t0517210_e0518437_b15037_c20201013052249869932_cspp_dev.h5'},  # noqa
                                  {
                                      'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVM01_j01_d20201013_t0517210_e0518437_b15037_c20201013052306592492_cspp_dev.h5',  # noqa
                                      'uid': 'SVM01_j01_d20201013_t0517210_e0518437_b15037_c20201013052306592492_cspp_dev.h5'},  # noqa
                                  {
                                      'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVM02_j01_d20201013_t0517210_e0518437_b15037_c20201013052306625314_cspp_dev.h5',  # noqa
                                      'uid': 'SVM02_j01_d20201013_t0517210_e0518437_b15037_c20201013052306625314_cspp_dev.h5'},  # noqa
                                  {
                                      'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVM03_j01_d20201013_t0517210_e0518437_b15037_c20201013052306658107_cspp_dev.h5',  # noqa
                                      'uid': 'SVM03_j01_d20201013_t0517210_e0518437_b15037_c20201013052306658107_cspp_dev.h5'},  # noqa
                                  {
                                      'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVM04_j01_d20201013_t0517210_e0518437_b15037_c20201013052306695328_cspp_dev.h5',  # noqa
                                      'uid': 'SVM04_j01_d20201013_t0517210_e0518437_b15037_c20201013052306695328_cspp_dev.h5'},  # noqa
                                  {
                                      'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVM05_j01_d20201013_t0517210_e0518437_b15037_c20201013052306731912_cspp_dev.h5',  # noqa
                                      'uid': 'SVM05_j01_d20201013_t0517210_e0518437_b15037_c20201013052306731912_cspp_dev.h5'},  # noqa
                                  {
                                      'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVM06_j01_d20201013_t0517210_e0518437_b15037_c20201013052306769387_cspp_dev.h5',  # noqa
                                      'uid': 'SVM06_j01_d20201013_t0517210_e0518437_b15037_c20201013052306769387_cspp_dev.h5'},  # noqa
                                  {
                                      'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVM07_j01_d20201013_t0517210_e0518437_b15037_c20201013052307952528_cspp_dev.h5',  # noqa
                                      'uid': 'SVM07_j01_d20201013_t0517210_e0518437_b15037_c20201013052307952528_cspp_dev.h5'},  # noqa
                                  {
                                      'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVM08_j01_d20201013_t0517210_e0518437_b15037_c20201013052306840611_cspp_dev.h5',  # noqa
                                      'uid': 'SVM08_j01_d20201013_t0517210_e0518437_b15037_c20201013052306840611_cspp_dev.h5'},  # noqa
                                  {
                                      'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVM09_j01_d20201013_t0517210_e0518437_b15037_c20201013052306876165_cspp_dev.h5',  # noqa
                                      'uid': 'SVM09_j01_d20201013_t0517210_e0518437_b15037_c20201013052306876165_cspp_dev.h5'},  # noqa
                                  {
                                      'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVM10_j01_d20201013_t0517210_e0518437_b15037_c20201013052306912966_cspp_dev.h5',  # noqa
                                      'uid': 'SVM10_j01_d20201013_t0517210_e0518437_b15037_c20201013052306912966_cspp_dev.h5'},  # noqa
                                  {
                                      'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVM11_j01_d20201013_t0517210_e0518437_b15037_c20201013052306946712_cspp_dev.h5',  # noqa
                                      'uid': 'SVM11_j01_d20201013_t0517210_e0518437_b15037_c20201013052306946712_cspp_dev.h5'},  # noqa
                                  {
                                      'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVM12_j01_d20201013_t0517210_e0518437_b15037_c20201013052306979943_cspp_dev.h5',  # noqa
                                      'uid': 'SVM12_j01_d20201013_t0517210_e0518437_b15037_c20201013052306979943_cspp_dev.h5'},  # noqa
                                  {
                                      'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVM13_j01_d20201013_t0517210_e0518437_b15037_c20201013052307008426_cspp_dev.h5',  # noqa
                                      'uid': 'SVM13_j01_d20201013_t0517210_e0518437_b15037_c20201013052307008426_cspp_dev.h5'},  # noqa
                                  {
                                      'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVM14_j01_d20201013_t0517210_e0518437_b15037_c20201013052307049977_cspp_dev.h5',  # noqa
                                      'uid': 'SVM14_j01_d20201013_t0517210_e0518437_b15037_c20201013052307049977_cspp_dev.h5'},  # noqa
                                  {
                                      'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVM15_j01_d20201013_t0517210_e0518437_b15037_c20201013052307083732_cspp_dev.h5',  # noqa
                                      'uid': 'SVM15_j01_d20201013_t0517210_e0518437_b15037_c20201013052307083732_cspp_dev.h5'},  # noqa
                                  {
                                      'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVM16_j01_d20201013_t0517210_e0518437_b15037_c20201013052307116885_cspp_dev.h5',  # noqa
                                      'uid': 'SVM16_j01_d20201013_t0517210_e0518437_b15037_c20201013052307116885_cspp_dev.h5'},  # noqa
                                  {
                                      'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/GIMGO_j01_d20201013_t0517210_e0518437_b15037_c20201013052249453008_cspp_dev.h5',  # noqa
                                      'uid': 'GIMGO_j01_d20201013_t0517210_e0518437_b15037_c20201013052249453008_cspp_dev.h5'},  # noqa
                                  {
                                      'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/GITCO_j01_d20201013_t0517210_e0518437_b15037_c20201013052249043730_cspp_dev.h5',  # noqa
                                      'uid': 'GITCO_j01_d20201013_t0517210_e0518437_b15037_c20201013052249043730_cspp_dev.h5'},  # noqa
                                  {
                                      'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVI01_j01_d20201013_t0517210_e0518437_b15037_c20201013052306211501_cspp_dev.h5',  # noqa
                                      'uid': 'SVI01_j01_d20201013_t0517210_e0518437_b15037_c20201013052306211501_cspp_dev.h5'},  # noqa
                                  {
                                      'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVI02_j01_d20201013_t0517210_e0518437_b15037_c20201013052306288882_cspp_dev.h5',  # noqa
                                      'uid': 'SVI02_j01_d20201013_t0517210_e0518437_b15037_c20201013052306288882_cspp_dev.h5'},  # noqa
                                  {
                                      'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVI03_j01_d20201013_t0517210_e0518437_b15037_c20201013052306364990_cspp_dev.h5',  # noqa
                                      'uid': 'SVI03_j01_d20201013_t0517210_e0518437_b15037_c20201013052306364990_cspp_dev.h5'},  # noqa
                                  {
                                      'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVI04_j01_d20201013_t0517210_e0518437_b15037_c20201013052306440875_cspp_dev.h5',  # noqa
                                      'uid': 'SVI04_j01_d20201013_t0517210_e0518437_b15037_c20201013052306440875_cspp_dev.h5'},  # noqa
                                  {
                                      'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVI05_j01_d20201013_t0517210_e0518437_b15037_c20201013052306516433_cspp_dev.h5',  # noqa
                                      'uid': 'SVI05_j01_d20201013_t0517210_e0518437_b15037_c20201013052306516433_cspp_dev.h5'},  # noqa
                                  {
                                      'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/GDNBO_j01_d20201013_t0517210_e0518437_b15037_c20201013052248852780_cspp_dev.h5',  # noqa
                                      'uid': 'GDNBO_j01_d20201013_t0517210_e0518437_b15037_c20201013052248852780_cspp_dev.h5'},  # noqa
                                  {
                                      'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/SVDNB_j01_d20201013_t0517210_e0518437_b15037_c20201013052306011816_cspp_dev.h5',  # noqa
                                      'uid': 'SVDNB_j01_d20201013_t0517210_e0518437_b15037_c20201013052306011816_cspp_dev.h5'},  # noqa
                                  {
                                      'uri': 'ssh://lxserv1043.smhi.se/san1/polar_in/direct_readout/npp/lvl1/noaa20_20201013_0517_15037/IVCDB_j01_d20201013_t0517210_e0518437_b15037_c20201013052306046518_cspu_pop.h5',  # noqa
                                      'uid': 'IVCDB_j01_d20201013_t0517210_e0518437_b15037_c20201013052306046518_cspu_pop.h5'}]}  # noqa


pps_message = ('pytroll://foo/pps/segment/collection/CF/2/CloudProducts/ dataset safusr.u@lxserv1043.smhi.se '
               '2020-09-11T12:36:48.777429 v1.01 application/json {"orig_platform_name": "noaa20", "orbit_number": '
               '14587, "start_time": "2020-09-11T12:05:08.400000", "stfrac": 4, "end_time": '
               '"2020-09-11T12:06:31.200000", "etfrac": 2, "module": "ppsMakePhysiography", "pps_version": "v2018", '
               '"platform_name": "NOAA-20", "orbit": 14587, "file_was_already_processed": false, '
               '"data_processing_level": "2", "format": "CF", "status": "OK", "dataset": [{"uri": '
               '"/san1/polar_out/direct_readout/lvl2/S_NWC_CMA_noaa20_14587_20200911T1205084Z_20200911T1206312Z.nc", '
               '"uid": "S_NWC_CMA_noaa20_14587_20200911T1205084Z_20200911T1206312Z.nc"}, {"uri": '
               '"/san1/polar_out/direct_readout/lvl2/S_NWC_CTTH_noaa20_14587_20200911T1205084Z_20200911T1206312Z.nc", '
               '"uid": "S_NWC_CTTH_noaa20_14587_20200911T1205084Z_20200911T1206312Z.nc"}, {"uri": '
               '"/san1/polar_out/direct_readout/lvl2/S_NWC_CT_noaa20_14587_20200911T1205084Z_20200911T1206312Z.nc", '
               '"uid": "S_NWC_CT_noaa20_14587_20200911T1205084Z_20200911T1206312Z.nc"}, {"uri": '
               '"/san1/polar_out/direct_readout/lvl2/S_NWC_CPP_noaa20_14587_20200911T1205084Z_20200911T1206312Z.nc", '
               '"uid": "S_NWC_CPP_noaa20_14587_20200911T1205084Z_20200911T1206312Z.nc"}], "sensor": ["viirs"]}')

pps_message_data = {'orig_platform_name': 'noaa20',
                    'orbit_number': 14587,
                    'start_time': dt.datetime(2020, 9, 11, 12, 5, 8, 400000),
                    'stfrac': 4,
                    'end_time': dt.datetime(2020, 9, 11, 12, 6, 31, 200000),
                    'etfrac': 2,
                    'module': 'ppsMakePhysiography',
                    'pps_version': 'v2018',
                    'platform_name': 'NOAA-20',
                    'orbit': 14587,
                    'file_was_already_processed': False,
                    'data_processing_level': '2',
                    'format': 'CF',
                    'status': 'OK',
                    'dataset': [{
                                    'uri': '/san1/polar_out/direct_readout/lvl2/S_NWC_CMA_noaa20_14587_20200911T1205084Z_20200911T1206312Z.nc',  # noqa
                                    'uid': 'S_NWC_CMA_noaa20_14587_20200911T1205084Z_20200911T1206312Z.nc'},
                                {
                                    'uri': '/san1/polar_out/direct_readout/lvl2/S_NWC_CTTH_noaa20_14587_20200911T1205084Z_20200911T1206312Z.nc',  # noqa
                                    'uid': 'S_NWC_CTTH_noaa20_14587_20200911T1205084Z_20200911T1206312Z.nc'},
                                {
                                    'uri': '/san1/polar_out/direct_readout/lvl2/S_NWC_CT_noaa20_14587_20200911T1205084Z_20200911T1206312Z.nc',  # noqa
                                    'uid': 'S_NWC_CT_noaa20_14587_20200911T1205084Z_20200911T1206312Z.nc'},
                                {
                                    'uri': '/san1/polar_out/direct_readout/lvl2/S_NWC_CPP_noaa20_14587_20200911T1205084Z_20200911T1206312Z.nc',  # noqa
                                    'uid': 'S_NWC_CPP_noaa20_14587_20200911T1205084Z_20200911T1206312Z.nc'}],
                    'sensor': ['viirs']}

pps_message = ('pytroll://foo/pps/segment/collection/CF/2/CloudProducts/ dataset safusr.u@lxserv1043.smhi.se '
               '2020-09-11T12:36:48.777429 v1.01 application/json {"orig_platform_name": "noaa20", "orbit_number": '
               '14587, "start_time": "2020-10-13T05:17:21.200000", "stfrac": 4, "end_time": '
               '"2020-09-11T05:18:43.700000", "etfrac": 2, "module": "ppsMakePhysiography", "pps_version": "v2018", '
               '"platform_name": "NOAA-20", "orbit": 14587, "file_was_already_processed": false, '
               '"data_processing_level": "2", "format": "CF", "status": "OK", "dataset": [{"uri": '
               '"/san1/polar_out/direct_readout/lvl2/S_NWC_CMA_noaa20_14587_20200911T1205084Z_20200911T1206312Z.nc", '
               '"uid": "S_NWC_CMA_noaa20_14587_20200911T1205084Z_20200911T1206312Z.nc"}, {"uri": '
               '"/san1/polar_out/direct_readout/lvl2/S_NWC_CTTH_noaa20_14587_20200911T1205084Z_20200911T1206312Z.nc", '
               '"uid": "S_NWC_CTTH_noaa20_14587_20200911T1205084Z_20200911T1206312Z.nc"}, {"uri": '
               '"/san1/polar_out/direct_readout/lvl2/S_NWC_CT_noaa20_14587_20200911T1205084Z_20200911T1206312Z.nc", '
               '"uid": "S_NWC_CT_noaa20_14587_20200911T1205084Z_20200911T1206312Z.nc"}, {"uri": '
               '"/san1/polar_out/direct_readout/lvl2/S_NWC_CPP_noaa20_14587_20200911T1205084Z_20200911T1206312Z.nc", '
               '"uid": "S_NWC_CPP_noaa20_14587_20200911T1205084Z_20200911T1206312Z.nc"}], "sensor": ["viirs"]}')


new_pps_message_data = \
                   {'orig_platform_name': 'noaa20',
                    'orbit_number': 15037,
                    'start_time': dt.datetime(2020, 10, 13, 5, 17, 21, 0),
                    'stfrac': 4,
                    'end_time': dt.datetime(2020, 10, 13, 5, 18, 43, 700000),
                    'etfrac': 2,
                    'module': 'ppsMakePhysiography',
                    'pps_version': 'v2018',
                    'platform_name': 'NOAA-20',
                    'orbit': 15037,
                    'file_was_already_processed': False,
                    'data_processing_level': '2',
                    'format': 'CF',
                    'status': 'OK',
                    'dataset': [{
                                    'uri': '/san1/polar_out/direct_readout/lvl2/S_NWC_CMA_noaa20_14587_20200911T1205084Z_20200911T1206312Z.nc',  # noqa
                                    'uid': 'S_NWC_CMA_noaa20_14587_20200911T1205084Z_20200911T1206312Z.nc'},
                                {
                                    'uri': '/san1/polar_out/direct_readout/lvl2/S_NWC_CTTH_noaa20_14587_20200911T1205084Z_20200911T1206312Z.nc',  # noqa
                                    'uid': 'S_NWC_CTTH_noaa20_14587_20200911T1205084Z_20200911T1206312Z.nc'},
                                {
                                    'uri': '/san1/polar_out/direct_readout/lvl2/S_NWC_CT_noaa20_14587_20200911T1205084Z_20200911T1206312Z.nc',  # noqa
                                    'uid': 'S_NWC_CT_noaa20_14587_20200911T1205084Z_20200911T1206312Z.nc'},
                                {
                                    'uri': '/san1/polar_out/direct_readout/lvl2/S_NWC_CPP_noaa20_14587_20200911T1205084Z_20200911T1206312Z.nc',  # noqa
                                    'uid': 'S_NWC_CPP_noaa20_14587_20200911T1205084Z_20200911T1206312Z.nc'}],
                    'sensor': ['viirs']}


@pytest.mark.usefixtures("caplog")
class TestSegmentGathererCollections:
    """Test collections gathering."""

    def setup_method(self):
        """Set up the test case."""
        self.collection_gatherer = SegmentGatherer(CONFIG_COLLECTIONS)

    def test_dataset_files_get_added_to_output_list(self):
        """Test dataset files get added to the output metadata."""
        from posttroll.message import Message as Message_p
        viirs_msg = Message_p(rawstr=viirs_message)

        self.collection_gatherer.process(viirs_msg)
        slot = self.collection_gatherer.slots['2020-10-13 05:17:21.200000']
        assert slot.output_metadata['collection']['viirs']['dataset'] == viirs_msg.data['dataset']
        assert "dataset" not in slot.output_metadata

    def test_collection_files_get_added_raises_not_implemented(self):
        """Test gathering a collection raises a not implemented error."""
        pps_msg = FakeMessage(pps_message_data, message_type='collection')
        with pytest.raises(NotImplementedError):
            self.collection_gatherer.process(pps_msg)

    def test_mismatching_files_generate_multiple_slots(self):
        """Test mismatching files generate multiple slots."""
        from posttroll.message import Message as Message_p
        viirs_msg = Message_p(rawstr=viirs_message)
        pps_msg = FakeMessage(pps_message_data, message_type='dataset')

        self.collection_gatherer.process(viirs_msg)
        self.collection_gatherer.process(pps_msg)

        assert len(self.collection_gatherer.slots) == 2

    def test_add_message_twice(self, caplog):
        """Test adding a message twice."""
        from posttroll.message import Message as Message_p
        viirs_msg = Message_p(rawstr=viirs_message)
        self.collection_gatherer.process(viirs_msg)
        with caplog.at_level(logging.DEBUG):
            self.collection_gatherer.process(viirs_msg)

            logs = [rec.message for rec in caplog.records]
            assert 'File already received' in logs

    def test_slot_is_ready(self, caplog):
        """Test when a slot is ready."""
        from posttroll.message import Message as Message_p
        viirs_msg = Message_p(rawstr=viirs_message)
        pps_msg = Message_p(rawstr=pps_message)

        with caplog.at_level(logging.DEBUG):
            self.collection_gatherer.process(viirs_msg)
            self.collection_gatherer.process(pps_msg)
        assert "Found existing time slot at 2020-10-13 05:17:21.200000, using that" in caplog.text

        slot = self.collection_gatherer.slots['2020-10-13 05:17:21.200000']
        assert slot.get_status() == Status.SLOT_READY

    def test_collection_is_published(self):
        """Test a collection is published."""
        from posttroll.message import Message as Message_p
        viirs_msg = Message_p(rawstr=viirs_message)
        pps_msg = Message_p(rawstr=pps_message)

        self.collection_gatherer.process(viirs_msg)
        self.collection_gatherer.process(pps_msg)

        slot = self.collection_gatherer.slots['2020-10-13 05:17:21.200000']
        self.collection_gatherer._publisher = MagicMock()
        self.collection_gatherer._subject = self.collection_gatherer._config['posttroll']['publish_topic']
        self.collection_gatherer.triage_slots()
        assert self.collection_gatherer._publisher.send.call_count == 1
        args, kwargs = self.collection_gatherer._publisher.send.call_args_list[0]
        message = Message_p(rawstr=args[0])
        assert message.data['collection'] == slot.output_metadata['collection']
        assert message.type == "collection"

    def test_slot_is_ready_when_timeout_expired(self):
        """Test when a slot is ready because of expired timeout."""
        from posttroll.message import Message as Message_p
        viirs_msg = Message_p(rawstr=viirs_message)

        self.collection_gatherer._timeliness = dt.timedelta(seconds=0)
        self.collection_gatherer.process(viirs_msg)

        slot = self.collection_gatherer.slots['2020-10-13 05:17:21.200000']
        assert slot.get_status() == Status.SLOT_READY

    def test_collection_is_published_when_timeout_expired(self):
        """Test a collection is published because of expired timeout."""
        from posttroll.message import Message as Message_p
        viirs_msg = Message_p(rawstr=viirs_message)

        self.collection_gatherer._timeliness = dt.timedelta(seconds=0)
        self.collection_gatherer.process(viirs_msg)

        slot = self.collection_gatherer.slots['2020-10-13 05:17:21.200000']
        self.collection_gatherer._publisher = MagicMock()
        self.collection_gatherer._subject = self.collection_gatherer._config['posttroll']['publish_topic']
        self.collection_gatherer.triage_slots()
        assert self.collection_gatherer._publisher.send.call_count == 1
        args, kwargs = self.collection_gatherer._publisher.send.call_args_list[0]
        message = Message_p(rawstr=args[0])
        assert message.data['collection'] == slot.output_metadata['collection']
        assert message.type == "collection"

    def test_bundled_dataset_is_published(self):
        """Test one bundled dataset is published."""
        from posttroll.message import Message as Message_p
        viirs_msg = Message_p(rawstr=viirs_message)
        pps_msg = Message_p(rawstr=pps_message)

        self.collection_gatherer.process(viirs_msg)
        self.collection_gatherer.process(pps_msg)

        slot = self.collection_gatherer.slots['2020-10-13 05:17:21.200000']
        self.collection_gatherer._bundle_datasets = True
        self.collection_gatherer._publisher = MagicMock()
        self.collection_gatherer._subject = self.collection_gatherer._config['posttroll']['publish_topic']
        self.collection_gatherer.triage_slots()
        assert self.collection_gatherer._publisher.send.call_count == 1
        args, kwargs = self.collection_gatherer._publisher.send.call_args_list[0]
        message = Message_p(rawstr=args[0])
        assert "dataset" in message.data
        dataset = []
        for files in slot.output_metadata['collection'].values():
            dataset.extend(files['dataset'])
        assert message.data['dataset'] == dataset
        assert message.type == "dataset"

    def test_end_time_correct_group_by_minutes(self):
        """Test that end_time is correct in message."""
        sg = SegmentGatherer(fake_config)
        messages = [posttroll.message.Message(
            rawstr=f"pytroll://tree/oak file pytroll@forest 1980-01-01T13:0{i:d}:00.000000 v1.01 application/json "
                   f'{{"platform_name": "forest", "start_time": "1980-01-01T13:0{i:d}:00", "end_time": '
                   f'"1980-01-01T13:0{i+1:d}:00", "uri": "/data/oak-s19800101130{i:d}00-e19800101130{i+1:d}00-'
                   f's00{i:d}.tree", "uid": "oak-s19800101130{i:d}00-e19800101130{i+1:d}00-s00{i:d}.tree", '
                   '"sensor": "Thaumetopoea processionea"}')
                for i in range(3)]
        for msg in messages:
            sg.process(msg)
        assert len(sg.slots) == 1
        assert sg.slots["1980-01-01 13:00:00"].output_metadata["start_time"] == dt.datetime(1980, 1, 1, 13, 0, 0)
        assert sg.slots["1980-01-01 13:00:00"].output_metadata["end_time"] == dt.datetime(1980, 1, 1, 13, 3, 0)


pps_message1 = ('pytroll://segment/CF/2/CMA/norrkoping/utv/polar/direct_readout/ file safusr.u@lxserv1043.smhi.se '
                '2020-10-16T08:01:59.035595 v1.01 application/json {"module": "ppsCmask", "pps_version": "v2018", '
                '"platform_name": "NOAA-20", "orbit": 15081, "sensor": "viirs", "start_time": '
                '"2020-10-16T07:48:15.300000", "end_time": "2020-10-16T07:49:39.800000", "file_was_already_processed": '
                'false, "uri": '
                '"ssh://lxserv1043.smhi.se/san1/polar_out/direct_readout/lvl2/S_NWC_CMA_noaa20_15081_20201016T0748153Z_20201016T0749398Z.nc"'  # noqa
                ', "uid": "S_NWC_CMA_noaa20_15081_20201016T0748153Z_20201016T0749398Z.nc", "data_processing_level": '
                '"2", "format": "CF", "station": "norrkoping", "posttroll_topic": "PPSv2018", "variant": "DR"}')
pps_message2 = ('pytroll://segment/CF/2/CTTH/norrkoping/utv/polar/direct_readout/ file safusr.u@lxserv1043.smhi.se '
                '2020-10-16T08:02:30.173279 v1.01 application/json {"module": "ppsCtth", "pps_version": "v2018", '
                '"platform_name": "NOAA-20", "orbit": 15081, "sensor": "viirs", "start_time": '
                '"2020-10-16T07:48:15.300000", "end_time": "2020-10-16T07:49:39.800000", "file_was_already_processed": '
                'false, "uri": '
                '"ssh://lxserv1043.smhi.se/san1/polar_out/direct_readout/lvl2/S_NWC_CTTH_noaa20_15081_20201016T0748153Z_20201016T0749398Z.nc"'  # noqa
                ', "uid": "S_NWC_CTTH_noaa20_15081_20201016T0748153Z_20201016T0749398Z.nc", "data_processing_level": '
                '"2", "format": "CF", "station": "norrkoping", "posttroll_topic": "PPSv2018", "variant": "DR"}')


class TestStartTimes:
    """Test incomplete start times."""

    def test_incomplete_start_times(self):
        """Test incomplete start times."""
        from posttroll.message import Message as Message_p
        collection_gatherer = SegmentGatherer(CONFIG_PPS)
        pps_msg1 = Message_p(rawstr=pps_message1)
        pps_msg2 = Message_p(rawstr=pps_message2)
        collection_gatherer.process(pps_msg1)
        collection_gatherer.process(pps_msg2)

        assert len(collection_gatherer.slots) == 1
        assert len(list(collection_gatherer.slots.values())[0].output_metadata['dataset']) == 2


class TestMessage:
    """Test the message object."""

    def test_create_message(self):
        """Test message creation."""
        fake_message = FakeMessage(pps_message_data)
        self.collection_gatherer = SegmentGatherer(CONFIG_COLLECTIONS)
        message = Message(fake_message, self.collection_gatherer._patterns['pps'])
        assert message

    def test_message_from_posttroll(self):
        """Test creating a message from a posttroll message."""
        fake_message = FakeMessage(pps_message_data)
        self.collection_gatherer = SegmentGatherer(CONFIG_COLLECTIONS)
        assert isinstance(self.collection_gatherer.message_from_posttroll(fake_message), Message)

    def test_message_type_dataset(self):
        """Test creating a message from a posttroll message gives the right type."""
        self.collection_gatherer = SegmentGatherer(CONFIG_COLLECTIONS)
        fake_message = FakeMessage(pps_message_data, 'dataset')
        assert self.collection_gatherer.message_from_posttroll(fake_message).type == 'dataset'

    def test_message_type_collection(self):
        """Test creating a message from a posttroll message gives the right type."""
        self.collection_gatherer = SegmentGatherer(CONFIG_COLLECTIONS)
        fake_message = FakeMessage(viirs_message_data, 'collection')
        assert self.collection_gatherer.message_from_posttroll(fake_message).type == 'collection'

    def test_id_time(self):
        """Test id time."""
        fake_message = FakeMessage(pps_message_data)
        self.collection_gatherer = SegmentGatherer(CONFIG_COLLECTIONS)
        message = self.collection_gatherer.message_from_posttroll(fake_message)
        assert message.id_time == dt.datetime(2020, 9, 11, 12, 5, 8, 400000)

    def test_get_unique_id_from_file_message(self):
        """Test getting a unique id from a message."""
        mda_msg0deg = {"segment": "EPI", "uid": "H-000-MSG3__-MSG3________-_________-EPI______-201611281100-__",
                       "platform_shortname": "MSG3", "start_time": dt.datetime(2016, 11, 28, 11, 0, 0),
                       "nominal_time": dt.datetime(2016, 11, 28, 11, 0, 0),
                       "uri": "/home/lahtinep/data/satellite/geo/msg/H-000-MSG3__-MSG3________-_________-EPI______-201611281100-__",  # noqa
                       "platform_name": "Meteosat-10", "channel_name": "", "path": "", "sensor": ["seviri"],
                       "hrit_format": "MSG3"}
        gatherer = SegmentGatherer(CONFIG_SINGLE)
        fake_message = FakeMessage(mda_msg0deg)
        message = gatherer.message_from_posttroll(fake_message)
        assert message.uid() == mda_msg0deg['uid']

    def test_get_unique_id_from_dataset_message(self):
        """Test getting unique id from a dataset message."""
        fake_message = FakeMessage(viirs_message_data)
        self.collection_gatherer = SegmentGatherer(CONFIG_COLLECTIONS)
        message = self.collection_gatherer.message_from_posttroll(fake_message)
        assert message.uid().startswith('NOAA-20_2020-10-13 05:17:21')


class TestFlooring:
    """Test flooring."""

    def setup_method(self):
        """Set up the test case."""
        self.himawari_ini = SegmentGatherer(CONFIG_INI_HIMAWARI)

        self.himawari_msg = FakeMessage({'uid': "IMG_DK01IR4_201712081129_010"})
        self.himawari_ini_message = self.himawari_ini.message_from_posttroll(self.himawari_msg)

    def test_floor_time_10_minutes(self):
        """Test flooring 10 minutes."""
        self.himawari_ini_message.adjust_time_by_flooring()
        assert self.himawari_ini_message.metadata['start_time'].minute == 20

    def test_floor_time_15_minutes(self):
        """Test flooring 15 minutes."""
        with patch.dict(CONFIG_INI_HIMAWARI, {'group_by_minutes': 15}):
            self.himawari_ini = SegmentGatherer(CONFIG_INI_HIMAWARI)
            self.himawari_ini_message = self.himawari_ini.message_from_posttroll(self.himawari_msg)

            self.himawari_ini_message.adjust_time_by_flooring()
            assert self.himawari_ini_message.metadata['start_time'].minute == 15

    def test_floor_time_2_minutes(self):
        """Test flooring 2 minutes."""
        with patch.dict(CONFIG_INI_HIMAWARI, {'group_by_minutes': 2}):
            self.himawari_ini = SegmentGatherer(CONFIG_INI_HIMAWARI)
            self.himawari_ini_message = self.himawari_ini.message_from_posttroll(self.himawari_msg)

            self.himawari_ini_message.adjust_time_by_flooring()
            assert self.himawari_ini_message.metadata['start_time'].minute == 28

    def test_floor_time_2_minutes_with_seconds(self):
        """Test flooring 2 minutes with zeroing of seconds."""
        with patch.dict(CONFIG_INI_HIMAWARI, {'group_by_minutes': 2}):
            self.himawari_ini = SegmentGatherer(CONFIG_INI_HIMAWARI)
            self.himawari_ini_message = self.himawari_ini.message_from_posttroll(self.himawari_msg)

            start_time = self.himawari_ini_message.metadata['start_time']
            self.himawari_ini_message.metadata['start_time'] = dt.datetime(start_time.year, start_time.month,
                                                                           start_time.day, start_time.hour,
                                                                           start_time.minute, 42)
            self.himawari_ini_message.adjust_time_by_flooring()
            assert self.himawari_ini_message.metadata['start_time'].minute == 28
            assert self.himawari_ini_message.metadata['start_time'].second == 0

    def test_floor_time_without_group_by_minutes_does_not_change_time(self):
        """Test that flooring the time without group_by_minutes defined keeps the time intact."""
        with patch.dict(CONFIG_INI_HIMAWARI, {'group_by_minutes': None}):
            self.himawari_ini = SegmentGatherer(CONFIG_INI_HIMAWARI)
            self.himawari_ini_message = self.himawari_ini.message_from_posttroll(self.himawari_msg)
            self.himawari_ini_message.adjust_time_by_flooring()
            assert self.himawari_ini_message.metadata['start_time'].minute == 29


class TestFlooringMultiplePatterns:
    """Test flooring."""

    def setup_method(self):
        """Set up the test case."""
        self.iodc_himawari = SegmentGatherer(CONFIG_DOUBLE_DIFFERENT)

        self.himawari_msg = FakeMessage({'uid': "IMG_DK01IR4_201712081129_010"})
        self.himawari_message = self.iodc_himawari.message_from_posttroll(self.himawari_msg)
        self.iodc_msg = FakeMessage({'uid': "H-000-MSG2__-MSG2_IODC___-_________-EPI______-201611281115-__"})
        self.iodc_message = self.iodc_himawari.message_from_posttroll(self.iodc_msg)

    def test_parsing_minutes(self):
        """Test parsing the minutes."""
        assert self.iodc_message.metadata['start_time'].minute == 15

    def test_floor_10_minutes(self):
        """Test flooring by 10 minutes."""
        self.himawari_message.adjust_time_by_flooring()
        assert self.himawari_message.metadata['start_time'].minute == 20

    def test_floor_10_minutes_with_seconds_zeroed(self):
        """Test flooring by 10 minutes will zero seconds."""
        start_time = self.himawari_message.metadata['start_time']
        self.himawari_message.metadata['start_time'] = dt.datetime(start_time.year, start_time.month,
                                                                   start_time.day, start_time.hour,
                                                                   start_time.minute, 42)
        self.himawari_message.adjust_time_by_flooring()
        assert self.himawari_message.metadata['start_time'].minute == 20
        assert self.himawari_message.metadata['start_time'].second == 0

    def test_floor_time_without_group_by_minutes_does_not_change_time(self):
        """Test that flooring the time without global group_by_minutes still works."""
        with patch.dict(CONFIG_DOUBLE_DIFFERENT['patterns']['himawari'], {'group_by_minutes': None}):
            self.iodc_himawari = SegmentGatherer(CONFIG_DOUBLE_DIFFERENT)
            self.himawari_message = self.iodc_himawari.message_from_posttroll(self.himawari_msg)
            self.himawari_message.adjust_time_by_flooring()
            assert self.himawari_message.metadata['start_time'].minute == 29

    def test_floor_grouping_does_not_affect_other_pattern(self):
        """Test that `group_by_minutes` for one pattern doesn't leak to the other patterns."""
        assert self.iodc_message.metadata['start_time'].minute == 15
        self.iodc_message.adjust_time_by_flooring()
        assert self.iodc_message.metadata['start_time'].minute == 15


fci_message_1 = ('pytroll://segment/fci_nc file mraspaud@4730366dc313 2023-02-21T13:45:47.413168 v1.01 application/json'
                 ' {"path": "", "platform_name": "MTI1", "segment_type": "BODY", "processing_time": "20170920134515", '
                 '"start_time": "2017-09-20T13:40:08", "end_time": "2017-09-20T13:40:15", "special_compression": "JLS",'
                 ' "repeat_cycle_in_day": 83, "segment": "1", "uri": '
                 '"/mnt/input/W_XX-EUMETSAT-Darmstadt,IMG+SAT,MTI1+FCI-1C-RRAD-FDHSI-FD--CHK-BODY---NC4E_C_EUMT_20170920134515_GTT_DEV_20170920134008_20170920134015_N_JLS_T_0083_0001.nc", '  # noqa
                 '"uid": "W_XX-EUMETSAT-Darmstadt,IMG+SAT,MTI1+FCI-1C-RRAD-FDHSI-FD--CHK-BODY---NC4E_C_EUMT_20170920134515_GTT_DEV_20170920134008_20170920134015_N_JLS_T_0083_0001.nc", '  # noqa
                 '"sensor": ["fci"]}')


fci_message_2 = ('pytroll://segment/fci_nc file mraspaud@4730366dc313 2023-02-21T15:35:53.301660 v1.01 application/json'
                 ' {"path": "", "platform_name": "MTI1", "segment_type": "BODY", "processing_time": "20170920153527", '
                 '"start_time": "2017-09-20T15:30:09", "end_time": "2017-09-20T15:30:27", "special_compression": "JLS",'
                 ' "repeat_cycle_in_day": 83, "segment": "2", "uri": '
                 '"/mnt/input/W_XX-EUMETSAT-Darmstadt,IMG+SAT,MTI1+FCI-1C-RRAD-FDHSI-FD--CHK-BODY---NC4E_C_EUMT_20170920134527_GTT_DEV_20170920134009_20170920134027_N_JLS_T_0083_0002.nc",'  # noqa
                 ' "uid": "W_XX-EUMETSAT-Darmstadt,IMG+SAT,MTI1+FCI-1C-RRAD-FDHSI-FD--CHK-BODY---NC4E_C_EUMT_20170920134527_GTT_DEV_20170920134009_20170920134027_N_JLS_T_0083_0002.nc",'  # noqa
                 ' "sensor": ["fci"]}')


FCI_CONFIG = """patterns:
  fci_nc:
    pattern:
      "W_XX-EUMETSAT-Darmstadt,IMG+SAT,{platform_name:4s}+FCI-1C-RRAD-FDHSI-FD--CHK-{segment_type}---NC4E_C_EUMT_{processing_time}_GTT_DEV_{start_time:%Y%m%d%H%M%S}_{end_time:%Y%m%d%H%M%S}_N_{special_compression}_T_{repeat_cycle_in_day:>04d}_{segment:0>4s}.nc"
    critical_files:
    wanted_files: :0001-0040
    all_files: :0001-0040
    variable_tags: ["processing_time", "end_time", "segment_type"]
    group_by_minutes: 10
    time_tolerance: 600

timeliness:
  6000000

time_name:
  start_time

posttroll:
  topics:
    - /segment/fci_nc
  publish_topic:
    /dataset/fci_nc

"""


class TestSegmentGathererFCI:
    """Tests for the segment gatherer for FCI data."""

    def test_process_message_fci_with_nonzero_seconds(self, caplog):
        """Test processing a message."""
        from posttroll.message import Message as Message_p
        import yaml

        segment_gatherer = SegmentGatherer(yaml.safe_load(FCI_CONFIG))

        expected_uids = set()

        for fci_message in [fci_message_1, fci_message_2]:
            fci_msg = Message_p(rawstr=fci_message)
            expected_uids.add(fci_msg.data['uid'])
            msg = FakeMessage(fci_msg.data)
            segment_gatherer.process(msg)

        timestamp = '2017-09-20 13:40:00'
        assert timestamp in segment_gatherer.slots
        slot = segment_gatherer.slots[timestamp]
        assert len(slot["fci_nc"]["received_files"]) == 2
        assert len(slot.output_metadata["dataset"]) == 2
        uids = set(info["uid"] for info in slot.output_metadata["dataset"])
        assert uids == expected_uids
