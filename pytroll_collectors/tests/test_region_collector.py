#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012 - 2021 Pytroll developers
#
# Author(s):
#
#   Martin Raspaud <martin.raspaud@smhi.se>
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

"""Test region collector functionality."""

import logging
import pytest
import datetime
import unittest.mock
import io

yaml_europe = """
euro_ma:
  description: euro_ma
  projection:
    proj: stere
    lat_0: 45
    lon_0: 15
    k: 1
    x_0: 0
    y_0: 0
    ellps: WGS84
    no_defs: null
  shape:
    height: 1069
    width: 1538
  area_extent:
    lower_left_xy:
    - -3845890.2472199923
    - -2150868.4484187816
    upper_right_xy:
    - 3845890.2472199923
    - 3198354.325865823
    units: m
"""

tles = b"""
METOP-C
1 43689U 18087A   21101.60865186  .00000002  00000-0  20894-4 0  9998
2 43689  98.6928 163.0161 0002296 181.8672 178.2497 14.21491657125954
"""


_granule_metadata = {"platform_name": "Metop-C",
                     "sensor": "avhrr"}

_granule_metadata_metop_b = {"platform_name": "Metop-B",
                             "sensor": "avhrr"}

def granule_metadata(s_min):
    """Return common granule_metadata dictionary."""
    return {**_granule_metadata,
            "start_time": datetime.datetime(2021, 4, 11, 10, s_min, 0),
            "end_time": datetime.datetime(2021, 4, 11, 10, s_min+3, 0),
            "uri": f"file://{s_min:d}"}


def granule_metadata_metop_b(s_min):
    """Return common granule_metadata dictionary."""
    return {**_granule_metadata_metop_b,
            "start_time": datetime.datetime(2021, 4, 11, 10, s_min, 0),
            "end_time": datetime.datetime(2021, 4, 11, 10, s_min+3, 0),
            "uri": f"file://{s_min:d}"}


def harvest_schedules(params, save_basename=None, eum_base_url=None):
    """Use this as fake harvester."""
    return None, None


@pytest.fixture
def europe():
    """Return european AreaDefinition."""
    from pyresample.area_config import load_area_from_string
    return load_area_from_string(yaml_europe)


@pytest.fixture
def europe_collector(europe):
    """Construct RegionCollector for Central Europe."""
    from pytroll_collectors.region_collector import RegionCollector
    return RegionCollector(europe)


@pytest.fixture
def europe_collector_schedule_cut(europe, schedule_cut=True):
    """Construct RegionCollector for Central Europe with schedule cut."""
    from pytroll_collectors.region_collector import RegionCollector
    return RegionCollector(europe, schedule_cut=schedule_cut)


@pytest.fixture
def europe_collector_schedule_cut_custom_method(europe, schedule_cut=True,
                                                schedule_cut_method='pytroll_collectors.tests.test_region_collector'):
    """Construct RegionCollector for Central Europe with schedule cut."""
    from pytroll_collectors.region_collector import RegionCollector
    return RegionCollector(europe, schedule_cut=schedule_cut, schedule_cut_method=schedule_cut_method)


@pytest.fixture
def europe_collector_schedule_cut_custom_method_failed(europe, schedule_cut=True,
                                                       schedule_cut_method='failed_not_existing_module'):
    """Construct RegionCollector for Central Europe with schedule cut."""
    from pytroll_collectors.region_collector import RegionCollector
    return RegionCollector(europe, schedule_cut=schedule_cut, schedule_cut_method=schedule_cut_method)


def _fakeopen(url):
    return io.BytesIO(tles)


def test_init(europe):
    """Test that initialisation appears to work."""
    from pytroll_collectors.region_collector import RegionCollector
    RegionCollector(europe)


@unittest.mock.patch("pyorbital.tlefile.urlopen", new=_fakeopen)
def test_collect(europe_collector, caplog):
    """Test that granules can be collected."""
    with caplog.at_level(logging.DEBUG):
        for s_min in (0, 3, 6, 9, 12, 15, 18):
            europe_collector.collect({**granule_metadata(s_min)})

    assert "Granule file://0 is overlapping region euro_ma by fraction" in caplog.text
    assert "Added new overlapping granule Metop-C (2021-04-11 10:00:00) to area euro_ma" in caplog.text
    assert "Collection finished for Metop-C area euro_ma" in caplog.text
    for n in (3, 6, 9, 12, 15):
        assert f"Added expected granule Metop-C (2021-04-11 10:{n:>02d}:00) to area euro_ma" in caplog.text
    assert "Granule file://18 is not overlapping euro_ma"


@unittest.mock.patch("pyorbital.tlefile.urlopen", new=_fakeopen)
def test_collect_duration(europe):
    """Test with tle_platform_name, without end_time, using call syntax."""
    from pytroll_collectors.region_collector import RegionCollector
    alt_europe_collector = RegionCollector(
            europe,
            timeliness=datetime.timedelta(seconds=300),
            granule_duration=datetime.timedelta(seconds=120))
    granule_metadata = {
            "sensor": ["avhrr"],
            "tle_platform_name": "Metop-C",
            "start_time": datetime.datetime(2021, 4, 11, 0, 0)}
    alt_europe_collector(granule_metadata)


@unittest.mock.patch("pyorbital.tlefile.urlopen", new=_fakeopen)
def test_collect_check_schedules(europe_collector_schedule_cut, caplog):
    """Test default schedule cut method."""
    with caplog.at_level(logging.DEBUG):
        for s_min in (0, 3, 6, 9, 12, 15, 18):
            europe_collector_schedule_cut.collect({**granule_metadata(s_min)})

    assert "Try import ['harvest_schedules'] module: pytroll_collectors.harvest_EUM_schedules" in caplog.text
    assert ("function : ['harvest_schedules'] loaded from module: "
            "pytroll_collectors.harvest_EUM_schedules") in caplog.text
    assert "Start harvest of cut schedules" in caplog.text
    assert "method: <module 'pytroll_collectors.harvest_EUM_schedules' from" in caplog.text
    assert "harvest_EUM_schedules.py'>, with type <class 'module'>" in caplog.text


@unittest.mock.patch("pyorbital.tlefile.urlopen", new=_fakeopen)
def test_collect_check_schedules_custom_method(europe_collector_schedule_cut_custom_method, caplog):
    """Test custom schedule cut method."""
    with caplog.at_level(logging.DEBUG):
        for s_min in (0, 3, 6, 9, 12, 15, 18):
            europe_collector_schedule_cut_custom_method.collect({**granule_metadata(s_min)})

    assert "Use custom schedule cut method provided in config file..." in caplog.text
    assert "method_name = pytroll_collectors.tests.test_region_collector" in caplog.text
    assert "Try import ['harvest_schedules'] module: pytroll_collectors.tests.test_region_collector" in caplog.text
    assert "loaded from module: pytroll_collectors.tests.test_region_collector" in caplog.text
    assert "Start harvest of cut schedules" in caplog.text
    assert "method: <module 'pytroll_collectors.tests.test_region_collector' from" in caplog.text
    assert "test_region_collector.py'>, with type <class 'module'>" in caplog.text


@unittest.mock.patch("pyorbital.tlefile.urlopen", new=_fakeopen)
def test_collect_check_schedules_custom_method_failed(europe_collector_schedule_cut_custom_method_failed, caplog):
    """Test custom schedule cut method failed import."""
    with caplog.at_level(logging.DEBUG):
        for s_min in (0, 3, 6, 9, 12, 15, 18):
            europe_collector_schedule_cut_custom_method_failed.collect({**granule_metadata(s_min)})

    test_string = ("Failed to import schedule_cut for harvest_schedules from failed_not_existing_module. "
                   "Will not perform schedule cut.")
    assert test_string in caplog.text


@unittest.mock.patch("pyorbital.tlefile.urlopen", new=_fakeopen)
def test_collect_missing_tle_from_file(europe_collector, caplog):
    """Test that granules can be collected, but missing TLE raises and exception"""
    with caplog.at_level(logging.DEBUG):
        for s_min in (0, 3, 6, 9, 12, 15, 18):
            with pytest.raises(KeyError):
                europe_collector.collect({**granule_metadata_metop_b(s_min)})


@unittest.mock.patch("pyorbital.tlefile.urlopen", new=_fakeopen)
def test_adjust_timeout(europe, caplog):
    """Test timeout adjustment."""
    from pytroll_collectors.region_collector import RegionCollector
    granule_metadata = {**_granule_metadata,
                        "uri": "file://alt/0"}
    alt_europe_collector = RegionCollector(
            europe,
            granule_duration=datetime.timedelta(seconds=180))

    with caplog.at_level(logging.DEBUG):
        alt_europe_collector.collect(
                {**granule_metadata,
                 "start_time": datetime.datetime(2021, 4, 11, 10, 0)})
        alt_europe_collector.collect(
                {**granule_metadata,
                 "start_time": datetime.datetime(2021, 4, 11, 10, 15)})
        alt_europe_collector.collect(
                {**granule_metadata,
                 "start_time": datetime.datetime(2021, 4, 11, 10, 12)})
    assert "Adjusted timeout" in caplog.text


@pytest.mark.skip(reason="test never finishes")
@unittest.mock.patch("pyorbital.tlefile.urlopen", new=_fakeopen)
def test_faulty_end_time(europe_collector, caplog):
    """Test adapting if end_time before start_time."""
    granule_metadata = {
        **_granule_metadata,
        "start_time": datetime.datetime(2021, 4, 11, 0, 0),
        "end_time": datetime.datetime(2021, 4, 10, 23, 58)}
    with caplog.at_level(logging.DEBUG):
        europe_collector(granule_metadata)
    assert "Adjusted end time" in caplog.text


def test_log_overlap_message_file_message(caplog):
    """Test logging a file message."""
    from pytroll_collectors.region_collector import _log_overlap_message

    granule_metadata = {'uri': 'filename'}
    with caplog.at_level(logging.DEBUG):
        _log_overlap_message(granule_metadata, "foobar")
    expected = "Granule filename foobar"
    assert expected in caplog.text


def test_log_overlap_message_dataset_message(caplog):
    """Test logging a dataset message."""
    from pytroll_collectors.region_collector import _log_overlap_message

    granule_metadata = {'dataset': [{'uri': 'filename1'}, {'uri': 'filename2'}],
                        'start_time': 1, 'end_time': 2}
    with caplog.at_level(logging.DEBUG):
        _log_overlap_message(granule_metadata, "foobar")
    expected = "Granule with start and end times = 1  2  foobar"
    assert expected in caplog.text


def test_log_overlap_message_backup_log_message(caplog):
    """Test the fallback log messaging."""
    from pytroll_collectors.region_collector import _log_overlap_message

    granule_metadata = {'key1': 'val1', 'key2': 'val2'}
    with caplog.at_level(logging.DEBUG):
        _log_overlap_message(granule_metadata, "foobar")
    assert "Failed printing debug info" in caplog.text
    assert "Keys in granule_metadata" in caplog.text
    assert "['key1', 'key2']" in caplog.text
