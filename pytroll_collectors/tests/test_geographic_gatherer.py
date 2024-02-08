#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2014 - 2021 Pytroll developers
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

"""Unittests for top level geographic segment gathering."""
import datetime as dt
import os
import time
from configparser import ConfigParser
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from pytroll_collectors.geographic_gatherer import arg_parse
from pytroll_collectors.triggers import PostTrollTrigger, WatchDogTrigger

AREA_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
AREA_DEFINITION_FILE = os.path.join(AREA_CONFIG_PATH, 'areas.yaml')


class FakeOpts(object):
    """Fake class to mimic commandline options."""

    def __init__(self, config_item=None, publish_port=0, nameservers=None):
        """Initialize."""
        self.config_item = config_item or []
        self.publish_port = publish_port
        self.nameservers = nameservers


class FakePostTrollTrigger(PostTrollTrigger):
    """Fake PostTrollTrigger."""

    def publish_collection(self, *args, **kwargs):
        """Publish collection."""
        del args, kwargs

    def start(self):
        """Start."""
        self.start_called = True


class FakeWatchDogTrigger(WatchDogTrigger):
    """Fake WatchDogTrigger."""

    def start(self):
        """Start."""
        self.start_called = True


@pytest.fixture
def tmp_config_file(tmp_path):
    """Return the path to the config file."""
    return tmp_path / "config.ini"


fake_create_publisher_from_dict_config = MagicMock()
fake_sub_factory = MagicMock()


@pytest.fixture
def tmp_config_parser():
    """Create a config parser for the geographic gatherer."""
    config = ConfigParser(interpolation=None)
    config['DEFAULT'] = {
        'regions': "euro4 euron1",
        'area_definition_file': AREA_DEFINITION_FILE}
    config['minimal_config'] = {
        'timeliness': '30',
        'service': 'service_a',
        'topics': 'topic_a',
    }
    config['posttroll_section'] = {
        'timeliness': '20',
        'service': 'service_b',
        'duration': '12.3',
        'topics': 'topic_b,topic_c',
        'watcher': 'posttroll',
        'publish_topic': '/topic',
        'inbound_connection': 'not_localhost, myhost:9999',
        'publish_message_after_each_reception': 'pmaer_is_yes'
    }
    config['polling_observer_section'] = {
        'timeliness': '10',
        'pattern': 'pattern',
        'publish_topic': '/topic',
        'watcher': 'PollingObserver',
    }
    config['observer_section'] = {
        'timeliness': '5',
        'pattern': 'pattern',
        'publish_topic': '/topic',
        'watcher': 'Observer',
    }
    return config


@patch('pytroll_collectors.geographic_gatherer.WatchDogTrigger', new=FakeWatchDogTrigger)
@patch('pytroll_collectors.geographic_gatherer.PostTrollTrigger', new=FakePostTrollTrigger)
@patch('pytroll_collectors.utils.create_publisher_from_dict_config', new=fake_create_publisher_from_dict_config)
@patch('pytroll_collectors.triggers._posttroll.create_subscriber_from_dict_config', new=fake_sub_factory)
class TestGeographicGatherer:
    """Test the top-level geographic gathering."""

    @pytest.fixture(autouse=True)
    def setup_method(self, tmp_config_file, tmp_config_parser):
        """Set up things."""
        self.config = tmp_config_parser

        fake_create_publisher_from_dict_config.reset_mock()
        fake_sub_factory.reset_mock()

        with open(tmp_config_file, mode="w") as fp:
            self.config.write(fp)

    def test_init_minimal(self, tmp_config_file):
        """Test initialization of GeographicGatherer with minimal config."""
        from pytroll_collectors.geographic_gatherer import GeographicGatherer

        section = 'minimal_config'
        opts = arg_parse(["-c", section, str(tmp_config_file)])
        gatherer = GeographicGatherer(opts)

        trigger = _check_one_trigger(gatherer, section)
        assert isinstance(trigger, FakePostTrollTrigger)

        # RegionCollector is called with two areas, the configured timeout and no duration
        timeliness = dt.timedelta(seconds=1800)
        duration = None
        self._check_region_collectors(trigger, section, timeliness, duration)

        self._check_publisher_no_args([section])

    def _check_region_collectors(self, trigger, section, timeliness, duration):
        from pyresample import parse_area_file
        for region, collector in zip(self.config.get(section, 'regions').split(), trigger.collectors):
            region_def = parse_area_file(AREA_DEFINITION_FILE, region)[0]
            assert collector.region == region_def
            assert collector.timeliness == timeliness
            assert collector.granule_duration == duration

    def test_init_minimal_no_nameservers(self, tmp_config_file):
        """Test initialization of GeographicGatherer with minimal config."""
        from pytroll_collectors.geographic_gatherer import GeographicGatherer

        section = 'minimal_config'
        opts = arg_parse(["-c", section, "-n", "false", "-p", "12345", str(tmp_config_file)])

        _ = GeographicGatherer(opts)
        # A publisher is created with composed name and started
        assert_create_publisher_from_dict_config([section], 12345, False)

    def test_init_no_area_def_file(self, tmp_config_file):
        """Test that GeographicGatherer gives a meaningful error message if area_definition_file is not defined."""
        import pytest
        from pytroll_collectors.geographic_gatherer import GeographicGatherer
        config = ConfigParser(interpolation=None)
        config.read(tmp_config_file)
        config.remove_option("DEFAULT", "area_definition_file")
        with open(tmp_config_file, mode="w") as fp:
            config.write(fp)
        # Make sure to work also when the environment has SATPY_CONFIG_PATH defined
        os.environ.pop("SATPY_CONFIG_PATH", None)
        section = 'minimal_config'
        opts = arg_parse(["-c", section, str(tmp_config_file)])

        with pytest.raises(KeyError) as err:
            _ = GeographicGatherer(opts)
        assert "'area_definition_file'" in str(err.value)

    def test_init_satpy_config_path(self, tmp_config_file):
        """Test that SATPY_CONFIG_PATH environment variable is used as default value if defined."""
        import os
        from pytroll_collectors.geographic_gatherer import GeographicGatherer
        self.config.remove_option("DEFAULT", "area_definition_file")
        section = 'minimal_config'
        opts = arg_parse(["-c", section, str(tmp_config_file)])

        os.environ["SATPY_CONFIG_PATH"] = AREA_CONFIG_PATH

        # This shouldn't raise anything
        _ = GeographicGatherer(opts)

    def test_init_posttroll(self, tmp_config_file):
        """Test initialization of GeographicGatherer for posttroll trigger."""
        from pytroll_collectors.geographic_gatherer import GeographicGatherer

        section = 'posttroll_section'
        opts = arg_parse(["-c", section, str(tmp_config_file)])

        gatherer = GeographicGatherer(opts)

        trigger = _check_one_trigger(gatherer, section)

        # The PostTrollTrigger is configured, so only that should've been called
        assert isinstance(trigger, FakePostTrollTrigger)

        services = self.config.get(section, 'service').split(',')
        topics = self.config.get(section, 'topics').split(',')
        duration = self.config.getfloat(section, 'duration')
        inbound_connection = self.config.get(section, "inbound_connection")
        nameserver, addresses = inbound_connection.split(",")
        addresses = ["tcp://" + addresses.strip()]

        sub_config = dict(services=services,
                          topics=topics,
                          nameserver=nameserver,
                          addr_listener=True,
                          addresses=addresses)
        fake_sub_factory.assert_called_once_with(sub_config)

        assert trigger.duration == duration

        self._check_trigger_publishing_info(trigger, section)

        publish_message_after_each_reception = self.config.get(section, 'publish_message_after_each_reception')
        assert trigger.publish_message_after_each_reception == publish_message_after_each_reception
        assert trigger.start_called

        # RegionCollector is called with two areas, the configured timeout and a duration
        timeliness = dt.timedelta(minutes=self.config.getint(section, "timeliness"))
        duration = dt.timedelta(seconds=12, microseconds=300000)
        self._check_region_collectors(trigger, section, timeliness, duration)

    def test_init_polling_observer(self, tmp_config_file):
        """Test initialization of GeographicGatherer for watchdog trigger as 'PollingObserver'."""
        from pytroll_collectors.geographic_gatherer import GeographicGatherer

        section = 'polling_observer_section'
        opts = arg_parse(["-c", section, str(tmp_config_file)])
        gatherer = GeographicGatherer(opts)

        self._watchdog_test(section, gatherer)

    def test_init_observer(self, tmp_config_file):
        """Test initialization of GeographicGatherer for watchdog trigger as 'Observer'."""
        from pytroll_collectors.geographic_gatherer import GeographicGatherer

        section = 'observer_section'
        opts = arg_parse(["-c", section, str(tmp_config_file)])
        gatherer = GeographicGatherer(opts)
        self._watchdog_test(section, gatherer)

    def _watchdog_test(self, section, gatherer):
        trigger = _check_one_trigger(gatherer, section)

        # The PollingObserver is configured, so only WatchDogTrigger should've been called
        assert isinstance(trigger, FakeWatchDogTrigger)

        assert trigger.wdp.patterns == ['pattern']
        watcher = gatherer._config.get(section, 'watcher')
        if watcher == "Observer":
            from watchdog.observers import Observer
            assert isinstance(trigger.wdp.observer, Observer)
        else:
            from watchdog.observers.polling import PollingObserver
            assert isinstance(trigger.wdp.observer, PollingObserver)

        self._check_trigger_publishing_info(trigger, section)

        assert trigger.start_called

        timeliness = dt.timedelta(minutes=self.config.getint(section, "timeliness"))
        duration = None
        self._check_region_collectors(trigger, section, timeliness, duration)

    def _check_trigger_publishing_info(self, trigger, section):
        assert trigger.publisher == fake_create_publisher_from_dict_config.return_value
        assert trigger.publish_topic == self.config.get(section, 'publish_topic')
        self._check_publisher_no_args([section])

    def _check_publisher_no_args(self, sections):
        assert_create_publisher_from_dict_config(sections, 0, None)

    def test_init_all_sections(self, tmp_config_file):
        """Test initialization of GeographicGatherer with all defined sections."""
        from pytroll_collectors.geographic_gatherer import GeographicGatherer

        opts = arg_parse(["-n", "nameserver_a", "-n", "nameserver_b", "-p", "9999", str(tmp_config_file)])

        gatherer = GeographicGatherer(opts)

        num_sections = len(self.config.sections())
        num_regions = len(self.config.get("DEFAULT", "regions").split())

        # All the sections should've created a trigger
        assert len(gatherer.triggers) == num_sections
        assert isinstance(gatherer.triggers[0], FakePostTrollTrigger)
        assert isinstance(gatherer.triggers[1], FakePostTrollTrigger)
        assert isinstance(gatherer.triggers[2], FakeWatchDogTrigger)
        assert isinstance(gatherer.triggers[3], FakeWatchDogTrigger)

        # N regions for each section
        assert all(len(trigger.collectors) == num_regions for trigger in gatherer.triggers)

        expected = {
            'name': 'gatherer',
            'port': 9999,
            'nameservers': ['nameserver_a', 'nameserver_b'],
        }
        fake_create_publisher_from_dict_config.assert_called_once_with(expected)
        fake_create_publisher_from_dict_config.return_value.start.assert_called_once()

    def test_fails_unreadable_config(self, tmp_path):
        """Test that it fails when the config is unreadable."""
        from pytroll_collectors.geographic_gatherer import GeographicGatherer
        opts = arg_parse(["/thıs/fıle/does/not/exıst"])
        with pytest.raises(
                OSError,
                match="Could not read configuration file /thıs/fıle/does/not/exıst"):
            GeographicGatherer(opts)
        os.chmod(os.fspath(tmp_path), 0)
        opts = arg_parse([os.fspath(tmp_path)])
        with pytest.raises(
                OSError,
                match=f"Could not read configuration file {tmp_path!s}"):
            GeographicGatherer(opts)


def assert_create_publisher_from_dict_config(sections, port, nameservers):
    """Check that publisher creator has been called correctly."""
    expected = {
        'name': 'gatherer_'+'_'.join(sections),
        'port': port,
        'nameservers': nameservers,
    }
    fake_create_publisher_from_dict_config.assert_called_once_with(expected)
    fake_create_publisher_from_dict_config.return_value.start.assert_called_once()


def _check_one_trigger(gatherer, section):
    # There's one trigger
    assert len(gatherer.triggers) == 1
    trigger = gatherer.triggers[0]
    # All the other sections should've been removed
    return trigger


@patch('pytroll_collectors.utils.create_publisher_from_dict_config', new=fake_create_publisher_from_dict_config)
class TestGeographicGathererWithPosttrollTrigger:
    """Test the top-level geographic gathering."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_config_file, tmp_config_parser):
        """Set up things."""
        self.config = tmp_config_parser

        fake_create_publisher_from_dict_config.reset_mock()

        with open(tmp_config_file, mode="w") as fp:
            self.config.write(fp)

    def test_args_accepts_inbound_info(self, tmp_config_file):
        """Test passing the inbound_connection arg."""
        section = 'posttroll_section'
        opts = arg_parse(["-c", section, "-i", "myhost:9999", str(tmp_config_file)])

        assert opts.inbound_connection == ["myhost:9999"]

    @patch('pytroll_collectors.geographic_gatherer.PostTrollTrigger')
    def test_posttroll_trigger_passes_inbound_info(self, posttroll_trigger_class, tmp_config_file):
        """Test that the host info is passed on to the posttroll trigger."""
        section = 'posttroll_section'
        host_info = "myhost:9999"
        opts = arg_parse(["-c", section, "-i", host_info, str(tmp_config_file)])

        from pytroll_collectors.geographic_gatherer import GeographicGatherer
        gatherer = GeographicGatherer(opts)
        _check_one_trigger(gatherer, section)

        assert (([host_info] in posttroll_trigger_class.mock_calls[0].args) or
                ([host_info] in posttroll_trigger_class.mock_calls[0].kwargs.values()))

    @patch('pytroll_collectors.geographic_gatherer.PostTrollTrigger')
    def test_posttroll_trigger_passes_multiple_inbound_info(self, posttroll_trigger_class, tmp_config_file):
        """Test that the multiple host info is passed on to the posttroll trigger."""
        section = 'posttroll_section'
        host_info = ["myhost:9999", "myotherhost:8888", "somenameserver"]
        opts = arg_parse(["-c", section,
                          "-i", host_info[0], "-i", host_info[1], "-i", host_info[2],
                          str(tmp_config_file)])

        from pytroll_collectors.geographic_gatherer import GeographicGatherer
        GeographicGatherer(opts)

        assert ((host_info in posttroll_trigger_class.mock_calls[0].args) or
                (host_info in posttroll_trigger_class.mock_calls[0].kwargs.values()))


class FakePass:
    """A fake Pass class."""

    def __init__(self, platform, start, end, instrument):
        """Set up the fake pass."""
        self.platform = platform
        self.start = start
        self.end = end
        self.instrument = instrument

    def area_coverage(self, area):
        """Compute fake area coverage."""
        del area
        if self.start.minute in range(43, 55):
            return 1
        return 0


@pytest.fixture
def tmp_tle(tmp_path):
    """Create a tle file."""
    filepath = tmp_path / "tle.txt"
    with open(filepath, mode='w') as fd:
        fd.write("NOAA 19\n"
                 "1 33591U 09005A   22137.89132437  .00000212  00000+0  13986-3 0  9991\n"
                 "2 33591  99.1504 171.3822 0012841 273.9417  86.0286 14.12563990683876\n")
    return filepath


@patch('pytroll_collectors.utils.create_publisher_from_dict_config', new=fake_create_publisher_from_dict_config)
@patch('pytroll_collectors.region_collector.Pass', new=FakePass)
class TestGeographicGathererWithPosttrollTriggerEndToEnd:
    """Test the top-level geographic gathering, end to end."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        """Set up things."""
        self.base_info = {"sensor": ["avhrr/3", "mhs", "amsu-a", "amsu-b", "hirs/4"],
                          "format": "HRPT",
                          "data_processing_level": "0",
                          "variant": "EARS",
                          "platform_name": "NOAA-19"}
        self.publish_topic = "/HRPT/0/NOAA-19/"

        config = ConfigParser(interpolation=None)
        config['DEFAULT'] = {
            'area_definition_file': AREA_DEFINITION_FILE}
        config['ears_avhrr'] = {
            'timeliness': '1500000000',
            'duration': '60',
            'service': '',
            'topics': '/segment/HRPT/0/NOAA-19/',
            'orbit_type': "polar",
            'regions': "euron1",
            "pattern": "/san1/polar_in/regional/avhrr/lvl0/avhrr_{start_time:%Y%m%d_%H%M%S}_{platform_name}.hrp",
            "publish_topic": self.publish_topic,
            "platform_name": self.base_info["platform_name"],
            "format": self.base_info["format"],
            "type": "binary",
            "data_processing_level": "0",
            "sensor": ",".join(self.base_info["sensor"]),
            "variant": self.base_info["variant"],
        }

        self.config = config

        fake_create_publisher_from_dict_config.reset_mock()

        self.config_path = tmp_path / 'config.ini'

        with open(self.config_path, mode="w") as fp:
            self.config.write(fp)

    @patch('pytroll_collectors.triggers._posttroll.create_subscriber_from_dict_config')
    def test_full_pass(self, sub_factory, monkeypatch, tmp_tle):
        """Test that the multiple host info is passed on to the posttroll trigger."""
        monkeypatch.setenv("TLES", os.fspath(tmp_tle))
        from posttroll.message import Message
        section = 'ears_avhrr'
        opts = arg_parse(["-c", section, "-v", str(self.config_path)])

        input_messages = []
        expected_files = []
        for minute in range(38, 57):
            expected_file = {"start_time": dt.datetime(2022, 5, 18, 9, minute, 0),
                             "end_time": dt.datetime(2022, 5, 18, 9, minute + 1, 0),
                             "uri": f"/EARS/avhrr_20220518_09{minute}00_noaa19.hrp.bz2",
                             "uid": f"avhrr_20220518_09{minute}00_noaa19.hrp.bz2"}
            expected_files.append(expected_file)

            input_data = expected_file.copy()
            input_data.update(self.base_info)
            input_messages.append(Message(self.publish_topic, "file", input_data))

        sub_factory.return_value.recv.return_value = input_messages

        from pytroll_collectors.geographic_gatherer import GeographicGatherer

        gatherer = GeographicGatherer(opts)
        try:
            time.sleep(.2)
            data = {"start_time": dt.datetime(2022, 5, 18, 9, 43, 0),
                    "end_time": dt.datetime(2022, 5, 18, 9, 55, 00),
                    "collection_area_id": "euron1",
                    "collection": expected_files
                    }
            data.update(self.base_info)
            expected_msg = Message(self.publish_topic, 'collection', data)

            ret = fake_create_publisher_from_dict_config.return_value
            snd_msg = Message(rawstr=ret.send.mock_calls[0].args[0])

            for snd_file in snd_msg.data["collection"]:
                assert snd_file in expected_msg.data["collection"]

            snd_msg.data.pop("collection")
            expected_msg.data.pop("collection")
            assert snd_msg.data == expected_msg.data
        finally:
            gatherer.stop()
