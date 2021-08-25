#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012 - 2021 Pytroll developers
#
# Author(s):
#
#   Kristian Rune Larsen <krl@dmi.dk>
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

"""Geographic segment gathering."""

import logging
import time
import datetime as dt

from configparser import NoOptionError
from posttroll import publisher
# Workaround for unit tests that don't need Satpy + Pyresample
try:
    from satpy.resample import get_area_def
except ImportError:
    get_area_def = None
from trollsift import Parser

from pytroll_collectors.region_collector import RegionCollector
from pytroll_collectors.triggers import PostTrollTrigger, WatchDogTrigger

logger = logging.getLogger(__name__)


class GeographicGatherer(object):
    """Container for granule triggers for geographic segment gathering."""

    def __init__(self, config, opts):
        """Initialize the class."""
        self._config = config
        self._opts = opts
        self.publisher = None
        self.triggers = []

        self._clean_config()
        self._setup_publisher()
        try:
            self._setup_triggers()
        except TypeError:
            raise ImportError("Satpy is required to run GeographicGatherer")

    def _clean_config(self):
        if self._opts.config_item:
            for section in self._opts.config_item:
                if section not in self._config.sections():
                    logger.warning(
                        "No config item called %s found in config file.", section)
            for section in self._config.sections():
                if section not in self._opts.config_item:
                    self._config.remove_section(section)
                    logger.debug("Removed unused section '%s'", section)
            if len(self._config.sections()) == 0:
                logger.error("No valid config item provided")
                raise NoOptionError

    def _setup_publisher(self):
        if self._opts.config_item:
            publisher_name = "gatherer_" + "_".join(self._opts.config_item)
        else:
            publisher_name = "gatherer"

        publish_port = self._opts.publish_port
        publisher_nameservers = self._opts.nameservers

        self.publisher = publisher.NoisyPublisher(publisher_name, port=publish_port,
                                                  nameservers=publisher_nameservers)
        self.publisher.start()

    def _setup_triggers(self):
        """Set up the granule triggers."""
        for section in self._config.sections():
            regions = [get_area_def(region)
                       for region in self._config.get(section, "regions").split()]
            collectors = self._get_collectors(section, regions)
            trigger = self._get_granule_trigger(section, collectors)
            trigger.start()
            self.triggers.append(trigger)

    def _get_collectors(self, section, regions):
        timeliness = dt.timedelta(minutes=self._config.getint(section, "timeliness"))
        try:
            duration = dt.timedelta(seconds=self._config.getfloat(section, "duration"))
        except NoOptionError:
            duration = None
        # Parse schedule cut if configured. Mainly for EARS data.
        try:
            schedule_cut = self._config.get(section, 'schedule_cut')
        except NoOptionError:
            schedule_cut = None
        # If you want to provide your own method to provide the schedule cut data
        try:
            schedule_cut_method = self._config.get(section, 'schedule_cut_method')
        except NoOptionError:
            schedule_cut_method = None

        return [RegionCollector(
            region, timeliness, duration, schedule_cut, schedule_cut_method)
            for region in regions]

    def _get_granule_trigger(self, section, collectors):
        try:
            observer_class = self._config.get(section, "watcher")
        except NoOptionError:
            observer_class = None

        if observer_class in ["PollingObserver", "Observer"]:
            granule_trigger = self._get_watchdog_trigger(section, observer_class, collectors)
        else:
            granule_trigger = self._get_posttroll_trigger(section, observer_class, collectors)

        return granule_trigger

    def _get_watchdog_trigger(self, section, observer_class, collectors):
        pattern = self._config.get(section, "pattern")
        parser = Parser(pattern)
        glob = parser.globify()
        publish_topic = self._get_publish_topic(section)

        logger.debug("Using %s for %s", observer_class, section)
        return WatchDogTrigger(
            collectors,
            self._config,
            [glob],
            observer_class,
            self.publisher,
            publish_topic=publish_topic)

    def _get_publish_topic(self, section):
        try:
            publish_topic = self._config.get(section, "publish_topic")
        except NoOptionError:
            publish_topic = None
        return publish_topic

    def _get_posttroll_trigger(self, section, observer_class, collectors):
        logger.debug("Using posttroll for %s", section)
        nameserver = self._get_nameserver(section)
        publish_topic = self._get_publish_topic(section)
        duration = self._get_duration(section)
        publish_message_after_each_reception = self._get_publish_message_after_each_reception(section)

        return PostTrollTrigger(
            collectors,
            self._config.get(section, 'service').split(','),
            self._config.get(section, 'topics').split(','),
            self.publisher,
            duration=duration,
            publish_topic=publish_topic, nameserver=nameserver,
            publish_message_after_each_reception=publish_message_after_each_reception)

    def _get_nameserver(self, section):
        try:
            nameserver = self._config.get(section, "nameserver")
        except NoOptionError:
            nameserver = "localhost"
        return nameserver

    def _get_duration(self, section):
        try:
            duration = self._config.getfloat(section, "duration")
        except NoOptionError:
            duration = None
        return duration

    def _get_publish_message_after_each_reception(self, section):
        try:
            publish_message_after_each_reception = self._config.get(section, "publish_message_after_each_reception")
            logger.debug("Publish message after each reception config: {}".format(publish_message_after_each_reception))
        except NoOptionError:
            publish_message_after_each_reception = False
        return publish_message_after_each_reception

    def run(self):
        """Run granule triggers."""
        try:
            while True:
                time.sleep(1)
                for trigger in self.triggers:
                    if not trigger.is_alive():
                        raise RuntimeError
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        except (RuntimeError, OSError):
            logger.exception('Something went wrong')
        finally:
            logger.info('Ending publication the gathering of granules...')
            for trigger in self.triggers:
                trigger.stop()
            self.publisher.stop()
