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

import unittest
from unittest.mock import patch, call, DEFAULT
from configparser import RawConfigParser
import datetime as dt
import os

AREA_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
AREA_DEFINITION_FILE = os.path.join(AREA_CONFIG_PATH, 'areas.yaml')


class FakeOpts(object):
    """Fake class to mimic commandline options."""

    def __init__(self, config_item=None, publish_port=0, nameservers=None):
        """Initialize."""
        self.config_item = config_item or []
        self.publish_port = publish_port
        self.nameservers = nameservers


class TestGeographicGatherer(unittest.TestCase):
    """Test the posttroll the top-level geographic gathering."""

    def setUp(self):
        """Set up things."""
        self.config = RawConfigParser()
        self.config['DEFAULT'] = {
            'regions': "euro4 euron1",
            'area_definition_file': AREA_DEFINITION_FILE}
        self.config['minimal_config'] = {
            'timeliness': '30',
            'service': 'service_a',
            'topics': 'topic_a',
            }
        self.config['posttroll_section'] = {
            'timeliness': '20',
            'service': 'service_b',
            'duration': '12.3',
            'topics': 'topic_b,topic_c',
            'watcher': 'posttroll',
            'publish_topic': '/topic',
            'nameserver': 'not_localhost',
            'publish_message_after_each_reception': 'pmaer_is_yes'
        }
        self.config['polling_observer_section'] = {
            'timeliness': '10',
            'pattern': 'pattern',
            'publish_topic': '/topic',
            'watcher': 'PollingObserver',
        }
        self.config['observer_section'] = {
            'timeliness': '5',
            'pattern': 'pattern',
            'publish_topic': '/topic',
            'watcher': 'Observer',
        }

        self.RegionCollector = self._patch_and_add_cleanup(
            'pytroll_collectors.geographic_gatherer.RegionCollector')
        self.WatchDogTrigger = self._patch_and_add_cleanup(
            'pytroll_collectors.geographic_gatherer.WatchDogTrigger')
        self.PostTrollTrigger = self._patch_and_add_cleanup(
            'pytroll_collectors.geographic_gatherer.PostTrollTrigger')
        self.create_publisher_from_dict_config = self._patch_and_add_cleanup(
            'pytroll_collectors.utils.create_publisher_from_dict_config')

    def _patch_and_add_cleanup(self, item, new=DEFAULT):
        patcher = patch(item, new=new)
        patched = patcher.start()
        self.addCleanup(patcher.stop)
        return patched

    def test_init_minimal(self):
        """Test initialization of GeographicGatherer with minimal config."""
        from pytroll_collectors.geographic_gatherer import GeographicGatherer
        from pyresample import parse_area_file

        sections = ['minimal_config']
        opts = FakeOpts(sections)
        gatherer = GeographicGatherer(self.config, opts)

        # There's one trigger
        assert len(gatherer.triggers) == 1

        # All the other sections should've been removed
        assert self.config.sections() == sections

        # The default is to use PostTrollTrigger, so only that should've been called
        self.PostTrollTrigger.assert_called_once()
        self.WatchDogTrigger.assert_not_called()

        # RegionCollector is called with two areas, the configured timeout and no duration
        for region in self.config.get(sections[0], 'regions').split():
            region_def = parse_area_file(AREA_DEFINITION_FILE, region)[0]
            assert call(region_def, dt.timedelta(seconds=1800), None, None, None) in self.RegionCollector.mock_calls

        # A publisher is created with composed name and started
        assert_create_publisher_from_dict_config(sections, 0, None, self.create_publisher_from_dict_config)

    def test_init_minimal_no_nameservers(self):
        """Test initialization of GeographicGatherer with minimal config."""
        from pytroll_collectors.geographic_gatherer import GeographicGatherer

        sections = ['minimal_config']
        opts = FakeOpts(sections, nameservers=['false'], publish_port=12345)
        _ = GeographicGatherer(self.config, opts)
        # A publisher is created with composed name and started
        assert_create_publisher_from_dict_config(sections, 12345, False, self.create_publisher_from_dict_config)

    def test_init_no_area_def_file(self):
        """Test that GeographicGatherer gives a meaningful error message if area_definition_file is not defined."""
        import pytest
        import os
        from configparser import NoOptionError
        from pytroll_collectors.geographic_gatherer import GeographicGatherer
        self.config.remove_option("DEFAULT", "area_definition_file")
        # Make sure to work also when the environment has SATPY_CONFIG_PATH defined
        os.environ.pop("SATPY_CONFIG_PATH", None)
        sections = ['minimal_config']
        opts = FakeOpts(sections)

        with pytest.raises(NoOptionError) as err:
            _ = GeographicGatherer(self.config, opts)
        assert "No option 'area_definition_file' in section" in str(err.value)

    def test_init_satpy_config_path(self):
        """Test that SATPY_CONFIG_PATH environment variable is used as defaulta value if defined."""
        import os
        from pytroll_collectors.geographic_gatherer import GeographicGatherer
        self.config.remove_option("DEFAULT", "area_definition_file")
        sections = ['minimal_config']
        opts = FakeOpts(sections)
        os.environ["SATPY_CONFIG_PATH"] = AREA_CONFIG_PATH

        # This shouldn't raise anything
        _ = GeographicGatherer(self.config, opts)

    def test_init_posttroll(self):
        """Test initialization of GeographicGatherer for posttroll trigger."""
        from pytroll_collectors.geographic_gatherer import GeographicGatherer
        from pyresample import parse_area_file

        sections = ['posttroll_section']
        opts = FakeOpts(sections)
        gatherer = GeographicGatherer(self.config, opts)

        # There's one trigger
        assert len(gatherer.triggers) == 1

        # All the other sections should've been removed
        assert self.config.sections() == sections

        # The PostTrollTrigger is configured, so only that should've been called
        self.PostTrollTrigger.assert_called_once()
        pt_call = call(
            [self.RegionCollector.return_value, self.RegionCollector.return_value],
            self.config.get(sections[0], 'service').split(','),
            self.config.get(sections[0], 'topics').split(','),
            self.create_publisher_from_dict_config.return_value,
            duration=self.config.getfloat(sections[0], 'duration'),
            publish_topic=self.config.get(sections[0], 'publish_topic'),
            nameserver=self.config.get(sections[0], 'nameserver'),
            publish_message_after_each_reception=self.config.get(sections[0], 'publish_message_after_each_reception'))
        assert pt_call in self.PostTrollTrigger.mock_calls
        self.PostTrollTrigger.return_value.start.assert_called_once()
        self.WatchDogTrigger.assert_not_called()

        # RegionCollector is called with two areas, the configured timeout and a duration
        for region in self.config.get(sections[0], 'regions').split():
            region_def = parse_area_file(AREA_DEFINITION_FILE, region)[0]
            assert call(
                region_def,
                dt.timedelta(seconds=1200),
                dt.timedelta(seconds=12, microseconds=300000), None, None) in self.RegionCollector.mock_calls

        # A publisher is created with composed name and started
        assert_create_publisher_from_dict_config(sections, 0, None, self.create_publisher_from_dict_config)

    def test_init_polling_observer(self):
        """Test initialization of GeographicGatherer for watchdog trigger as 'PollingObserver'."""
        from pytroll_collectors.geographic_gatherer import GeographicGatherer

        sections = ['polling_observer_section']
        opts = FakeOpts(sections)
        gatherer = GeographicGatherer(self.config, opts)

        self._watchdog_test(
            sections,
            gatherer,
            self.create_publisher_from_dict_config,
            self.PostTrollTrigger,
            self.WatchDogTrigger,
            self.RegionCollector,
        )

    def test_init_observer(self):
        """Test initialization of GeographicGatherer for watchdog trigger as 'Observer'."""
        from pytroll_collectors.geographic_gatherer import GeographicGatherer

        sections = ['observer_section']
        opts = FakeOpts(sections)
        gatherer = GeographicGatherer(self.config, opts)

        self._watchdog_test(
            sections,
            gatherer,
            self.create_publisher_from_dict_config,
            self.PostTrollTrigger,
            self.WatchDogTrigger,
            self.RegionCollector,
        )

    def _watchdog_test(self,
                       sections,
                       gatherer,
                       create_publisher_from_dict_config,
                       PostTrollTrigger,
                       WatchDogTrigger,
                       RegionCollector,
                       ):
        from pyresample import parse_area_file

        # There's one trigger
        assert len(gatherer.triggers) == 1

        # All the other sections should've been removed
        assert self.config.sections() == sections

        # The PollingObserver is configured, so only WatchDogTrigger should've been called
        WatchDogTrigger.assert_called_once()
        pt_call = call(
            [RegionCollector.return_value, RegionCollector.return_value],
            self.config,
            ['pattern'],
            self.config.get(sections[0], 'watcher'),
            create_publisher_from_dict_config.return_value,
            publish_topic=self.config.get(sections[0], 'publish_topic'))
        assert pt_call in WatchDogTrigger.mock_calls
        WatchDogTrigger.return_value.start.assert_called_once()
        PostTrollTrigger.assert_not_called()

        # RegionCollector is called with two areas, the configured timeout and a duration
        for region in self.config.get(sections[0], 'regions').split():
            region_def = parse_area_file(AREA_DEFINITION_FILE, region)[0]
            assert call(
                region_def,
                dt.timedelta(minutes=self.config.getint(sections[0], "timeliness")),
                None, None, None) in RegionCollector.mock_calls

        # A publisher is created with composed name and started
        assert_create_publisher_from_dict_config(sections, 0, None, create_publisher_from_dict_config)

    def test_init_all_sections(self):
        """Test initialization of GeographicGatherer with all defined sections."""
        from pytroll_collectors.geographic_gatherer import GeographicGatherer

        opts = FakeOpts(config_item=None, publish_port=9999, nameservers=['nameserver_a', 'nameserver_b'])
        gatherer = GeographicGatherer(self.config, opts)

        num_sections = len(self.config.sections())

        # All the sections should've created a trigger
        assert len(gatherer.triggers) == num_sections

        # See that the trigger classes have been accessed the correct times
        assert self.PostTrollTrigger.call_count == 2
        assert self.WatchDogTrigger.call_count == 2

        # N regions for each section
        assert self.RegionCollector.call_count == num_sections * len(self.config.get("DEFAULT", "regions").split())
        expected = {
            'name': 'gatherer',
            'port': 9999,
            'nameservers': ['nameserver_a', 'nameserver_b'],
        }
        self.create_publisher_from_dict_config.assert_called_once_with(expected)
        self.create_publisher_from_dict_config.return_value.start.assert_called_once()


def assert_create_publisher_from_dict_config(sections, port, nameservers, func):
    """Check that publisher creator has been called correctly."""
    expected = {
        'name': 'gatherer_'+'_'.join(sections),
        'port': port,
        'nameservers': nameservers,
    }
    func.assert_called_once_with(expected)
    func.return_value.start.assert_called_once()
