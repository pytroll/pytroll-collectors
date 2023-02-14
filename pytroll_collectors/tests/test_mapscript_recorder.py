"""Tests for the mapscript recorder."""
import yaml
import os

from pytroll_collectors.mapscript_recorder import record_for_mapscript, record_command
from posttroll.message import Message
import json

configuration = """
fields:
  uri: uri
  area_extent: areaname
  layer: "{service}_{productname}"
  start_time: start_time
aliases:
    areaname:
      euro4:
        - -2717181.7304994687
        - -5571048.14031214
        - 1378818.2695005313
        - -1475048.1403121399
    service:
      "___": "0deg"
"""

messages = [
    'pytroll://image/seviri_hrit file mraspaud@315e8f48f34a 2023-02-09T09:40:18.892494 v1.01 application/json '
    '{"orig_platform_name": "MSG4", "service": "___", "start_time": "2023-02-09T08:00:00", "compression": "_", '
    '"platform_name": "Meteosat-11", "sensor": ["seviri"], "uri": '
    '"/mnt/output/20230209_0800_Meteosat-11_euro4_overview.tif", "uid": "20230209_0800_Meteosat-11_euro4_overview.tif",'
    ' "product": "overview", "area": "euro4", "productname": "overview", "areaname": "euro4", "format": "tif"}',
    'pytroll://image/seviri_hrit file mraspaud@315e8f48f34a 2023-02-09T09:40:18.893347 v1.01 application/json '
    '{"orig_platform_name": "MSG4", "service": "___", "start_time": "2023-02-09T08:00:00", "compression": "_", '
    '"platform_name": "Meteosat-11", "sensor": ["seviri"], "uri": '
    '"/mnt/output/20230209_0800_Meteosat-11_euro4_airmass.tif", "uid": "20230209_0800_Meteosat-11_euro4_airmass.tif", '
    '"product": "airmass", "area": "euro4", "productname": "airmass", "areaname": "euro4", "format": "tif"}',
    'pytroll://image/seviri_hrit file mraspaud@315e8f48f34a 2023-02-09T09:40:18.894054 v1.01 application/json '
    '{"orig_platform_name": "MSG4", "service": "___", "start_time": "2023-02-09T08:00:00", "compression": "_", '
    '"platform_name": "Meteosat-11", "sensor": ["seviri"], "uri": '
    '"/mnt/output/20230209_0800_Meteosat-11_euro4_natural_color.tif", "uid": '
    '"20230209_0800_Meteosat-11_euro4_natural_color.tif", "product": "natural_color", "area": "euro4", "productname": '
    '"natural_color", "areaname": "euro4", "format": "tif"}']


def generate_messages():
    """Generate messages."""
    for msg in messages:
        yield Message(rawstr=msg)


def test_recorder_add_data_from_messages(tmp_path):
    """Test adding data to a file from some messages."""
    filename = tmp_path / "record.txt"
    config = f"filename: {filename}" + "\n" + configuration
    config = yaml.safe_load(config)
    record_for_mapscript(generate_messages, config)

    with open(filename, mode="r") as fd:
        for line in fd:
            line_data = json.loads(line)
            assert line_data["uri"].startswith("/mnt/output/20230209_0800_Meteosat-11_euro4_")
            assert line_data["layer"] in ["0deg_overview", "0deg_airmass", "0deg_natural_color"]
            assert line_data["area_extent"] == [-2717181.7304994687, -5571048.14031214,
                                                1378818.2695005313, -1475048.1403121399]


def test_recorder_args(tmp_path):
    """Test that the recorder command takes arguments."""
    config_file = tmp_path / "config_file.yaml"

    filename = tmp_path / "record.txt"
    config = f"filename: {filename}" + "\n" + configuration
    with open(config_file, "w") as fd:
        fd.write(config)
    record_command([os.fspath(config_file)], generate_messages)

    with open(filename, mode="r") as fd:
        for line in fd:
            line_data = json.loads(line)
            assert line_data["uri"].startswith("/mnt/output/20230209_0800_Meteosat-11_euro4_")
            assert line_data["layer"] in ["0deg_overview", "0deg_airmass", "0deg_natural_color"]
            assert line_data["area_extent"] == [-2717181.7304994687, -5571048.14031214,
                                                1378818.2695005313, -1475048.1403121399]
