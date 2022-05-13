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

from configparser import NoOptionError
from trollsift import Parser

from pytroll_collectors.region_collector import create_collectors_from_config_dict
from pytroll_collectors.triggers import PostTrollTrigger, WatchDogTrigger
from pytroll_collectors.utils import check_nameserver_options
from pytroll_collectors.utils import create_started_publisher_from_config
from pytroll_collectors.utils import create_publisher_config_dict

logger = logging.getLogger(__name__)


class GeographicGatherer:
    """Container for granule triggers for geographic segment gathering."""

    def __init__(self, config, opts):
        """Initialize the class."""
        self._config = config
        self._opts = opts
        self.publisher = None
        self.triggers = []
        self.return_status = 0

        self._clean_config()
        self._setup_publisher()
        try:
            self._setup_triggers()
        except NoOptionError:
            self.publisher.stop()
            raise

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
        self.publisher = create_started_publisher_from_config(self._collect_publisher_config())

    def _collect_publisher_config(self):
        if self._opts.config_item:
            publisher_name = "gatherer_" + "_".join(self._opts.config_item)
        else:
            publisher_name = "gatherer"

        publish_port = self._opts.publish_port
        publisher_nameservers = check_nameserver_options(self._opts.nameservers)

        return create_publisher_config_dict(publisher_name, publisher_nameservers, publish_port)

    def _setup_triggers(self):
        """Set up the granule triggers."""
        for section in self._config.sections():
            config_items = dict(self._config.items(section))
            collectors = create_collectors_from_config_dict(config_items)
            trigger = TriggerFactory(section, config_items, self.publisher).create(collectors)
            trigger.start()
            self.triggers.append(trigger)

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
            self.return_status = 1
        finally:
            logger.info('Ending publication the gathering of granules...')
            for trigger in self.triggers:
                trigger.stop()
            self.publisher.stop()

        return self.return_status


class TriggerFactory:
    """Factory for triggers."""

    def __init__(self, section, config_items, trigger_publisher):
        """Set up the factory."""
        self.section = section
        self._config_items = config_items
        self.publisher = trigger_publisher

    def create(self, collectors):
        """Create a trigger."""
        try:
            granule_trigger = self._get_watchdog_trigger(collectors)
        except (KeyError, ValueError):
            granule_trigger = self._get_posttroll_trigger(collectors)

        return granule_trigger

    def _get_watchdog_trigger(self, collectors):
        observer_class = self._config_items["watcher"]
        if observer_class not in ["PollingObserver", "Observer"]:
            raise ValueError

        pattern = self._config_items["pattern"]
        parser = Parser(pattern)
        glob = parser.globify()
        publish_topic = self._get_publish_topic()

        logger.debug("Using %s for %s", observer_class, self.section)
        return WatchDogTrigger(
            collectors,
            self._config_items,
            [glob],
            observer_class,
            self.publisher,
            publish_topic=publish_topic)

    def _get_publish_topic(self):
        return self._config_items.get("publish_topic")

    def _get_posttroll_trigger(self, collectors):
        logger.debug("Using posttroll for %s", self.section)
        nameserver = self._get_nameserver()
        publish_topic = self._get_publish_topic()
        duration = self._get_duration()
        publish_message_after_each_reception = self._get_publish_message_after_each_reception()

        return PostTrollTrigger(
            collectors,
            self._config_items['service'].split(','),
            self._config_items['topics'].split(','),
            self.publisher,
            duration=duration,
            publish_topic=publish_topic, nameserver=nameserver,
            publish_message_after_each_reception=publish_message_after_each_reception)

    def _get_nameserver(self):
        return self._config_items.get("nameserver", "localhost")

    def _get_duration(self):
        try:
            duration = float(self._config_items["duration"])
        except KeyError:
            duration = None
        return duration

    def _get_publish_message_after_each_reception(self):
        publish_message_after_each_reception = self._config_items.get("publish_message_after_each_reception", False)
        logger.debug("Publish message after each reception config: {}".format(publish_message_after_each_reception))
        return publish_message_after_each_reception
