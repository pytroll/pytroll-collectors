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


def test_init(europe):
    """Test that initialisation appears to work."""
    from pytroll_collectors.region_collector import RegionCollector
    RegionCollector(europe)


def test_collect(europe_collector, caplog):
    """Test that granules can be collected."""
    granule_metadata = {
            "platform_name": "Metop-C",
            "sensor": "avhrr"}

    def fakeopen(url):
        return io.BytesIO(tles)
    with unittest.mock.patch("pyorbital.tlefile.urlopen", new=fakeopen):
        # tu.return_value = io.BytesIO(tles)
        with caplog.at_level(logging.DEBUG):
            for s_min in (0, 3, 6, 9, 12, 15, 18):
                europe_collector.collect(
                        {**granule_metadata,
                         **{"start_time": datetime.datetime(2021, 4, 11, 10, s_min, 0),
                            "end_time": datetime.datetime(2021, 4, 11, 10, s_min+3, 0),
                            "uri": f"file://{s_min:d}"}})
    assert "Granule file://0 is overlapping region euro_ma by fraction 0.03685" in caplog.text
    assert "Added Metop-C (2021-04-11 10:00:00) granule to area euro_ma because it overlaps" in caplog.text
    assert "Collection finished for Metop-C area: euro_ma" in caplog.text
    for n in (3, 6, 9, 12, 15):
        assert f"Added Metop-C (2021-04-11 10:{n:>02d}:00) granule to area euro_ma because we expect it" in caplog.text
    assert "Granule file://18 is not overlapping euro_ma"
