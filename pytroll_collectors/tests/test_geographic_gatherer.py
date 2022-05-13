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
from unittest.mock import patch, DEFAULT
from configparser import ConfigParser
import datetime as dt
import os
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

    def publish_collection(self, *args, **kwargs):
        """Publish collection."""
        del args, kwargs

    def start(self):
        """Start."""
        self.start_called = True


class TestGeographicGatherer(unittest.TestCase):
    """Test the posttroll the top-level geographic gathering."""

    def setUp(self):
        """Set up things."""
        self.config = ConfigParser(interpolation=None)
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

        # self.RegionCollector = self._patch_and_add_cleanup(
        #     'pytroll_collectors.geographic_gatherer.RegionCollector')
        self.WatchDogTrigger = self._patch_and_add_cleanup(
            'pytroll_collectors.geographic_gatherer.WatchDogTrigger', new=FakeWatchDogTrigger)
        self.PostTrollTrigger = self._patch_and_add_cleanup(
            'pytroll_collectors.geographic_gatherer.PostTrollTrigger', new=FakePostTrollTrigger)
        self.publisher = self._patch_and_add_cleanup(
            'pytroll_collectors.geographic_gatherer.publisher')

        self.NSSubscriber = self._patch_and_add_cleanup(
            'pytroll_collectors.triggers._posttroll.NSSubscriber')

    def _patch_and_add_cleanup(self, item, new=DEFAULT):
        patcher = patch(item, new=new)
        patched = patcher.start()
        self.addCleanup(patcher.stop)
        return patched

    def test_init_minimal(self):
        """Test initialization of GeographicGatherer with minimal config."""
        from pytroll_collectors.geographic_gatherer import GeographicGatherer

        section = 'minimal_config'
        opts = FakeOpts([section])
        gatherer = GeographicGatherer(self.config, opts)

        trigger = self._check_one_trigger(gatherer, section)
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

    def test_init_no_area_def_file(self):
        """Test that GeographicGatherer gives a meaningful error message if area_definition_file is not defined."""
        import pytest
        from pytroll_collectors.geographic_gatherer import GeographicGatherer
        self.config.remove_option("DEFAULT", "area_definition_file")
        sections = ['minimal_config']
        opts = FakeOpts(sections)

        with pytest.raises(KeyError) as err:
            _ = GeographicGatherer(self.config, opts)
        assert "'area_definition_file'" in str(err.value)

    def test_init_satpy_config_path(self):
        """Test that SATPY_CONFIG_PATH environment variable is used as default value if defined."""
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

        section = 'posttroll_section'
        opts = FakeOpts([section])
        gatherer = GeographicGatherer(self.config, opts)

        trigger = self._check_one_trigger(gatherer, section)

        # The PostTrollTrigger is configured, so only that should've been called
        assert isinstance(trigger, FakePostTrollTrigger)

        services = self.config.get(section, 'service').split(',')
        topics = self.config.get(section, 'topics').split(',')
        duration = self.config.getfloat(section, 'duration')
        nameserver = self.config.get(section, 'nameserver')

        self.NSSubscriber.assert_called_once_with(services, topics, True, nameserver=nameserver)

        assert trigger.duration == duration

        self._check_trigger_publishing_info(trigger, section)

        publish_message_after_each_reception = self.config.get(section, 'publish_message_after_each_reception')
        assert trigger.publish_message_after_each_reception == publish_message_after_each_reception
        assert trigger.start_called

        # RegionCollector is called with two areas, the configured timeout and a duration
        timeliness = dt.timedelta(minutes=self.config.getint(section, "timeliness"))
        duration = dt.timedelta(seconds=12, microseconds=300000)
        self._check_region_collectors(trigger, section, timeliness, duration)

    def test_init_polling_observer(self):
        """Test initialization of GeographicGatherer for watchdog trigger as 'PollingObserver'."""
        from pytroll_collectors.geographic_gatherer import GeographicGatherer

        section = 'polling_observer_section'
        opts = FakeOpts([section])
        gatherer = GeographicGatherer(self.config, opts)

        self._watchdog_test(section, gatherer)

    def test_init_observer(self):
        """Test initialization of GeographicGatherer for watchdog trigger as 'Observer'."""
        from pytroll_collectors.geographic_gatherer import GeographicGatherer

        section = 'observer_section'
        opts = FakeOpts([section])
        gatherer = GeographicGatherer(self.config, opts)

        self._watchdog_test(section, gatherer)

    def _watchdog_test(self, section, gatherer):
        trigger = self._check_one_trigger(gatherer, section)

        # The PollingObserver is configured, so only WatchDogTrigger should've been called
        assert isinstance(trigger, FakeWatchDogTrigger)

        assert trigger.wdp.patterns == ['pattern']
        watcher = self.config.get(section, 'watcher')
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
        assert trigger.publisher == self.publisher.NoisyPublisher.return_value
        assert trigger.publish_topic == self.config.get(section, 'publish_topic')
        self._check_publisher_no_args([section])

    def _check_publisher_no_args(self, sections):
        # A publisher is created with composed name and started
        self.publisher.NoisyPublisher.assert_called_once_with('gatherer_' + '_'.join(sections), port=0,
                                                              nameservers=None)
        self.publisher.NoisyPublisher.return_value.start.assert_called_once()

    def _check_one_trigger(self, gatherer, section):
        # There's one trigger
        assert len(gatherer.triggers) == 1
        trigger = gatherer.triggers[0]
        # All the other sections should've been removed
        assert self.config.sections()[0] == section
        return trigger

    def test_init_all_sections(self):
        """Test initialization of GeographicGatherer with all defined sections."""
        from pytroll_collectors.geographic_gatherer import GeographicGatherer

        opts = FakeOpts(config_item=None, publish_port=9999, nameservers=['nameserver_a', 'nameserver_b'])
        gatherer = GeographicGatherer(self.config, opts)

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

        self.publisher.NoisyPublisher.assert_called_once_with(
            'gatherer',
            port=9999,
            nameservers=['nameserver_a', 'nameserver_b'])
        self.publisher.NoisyPublisher.return_value.start.assert_called_once()
