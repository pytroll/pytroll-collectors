#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012, 2014, 2015, 2019 Martin Raspaud
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

from pytroll_collectors.trigger import get_metadata, setup_triggers
from posttroll import publisher

logger = logging.getLogger(__name__)


class GeographicGatherer(object):
    """Container for granule triggers for geographich segment gathering."""

    def __init__(self, config, opts, decoder=get_metadata):
        """Initialize the class."""
        self._config = config
        self._opts = opts
        self.decoder = decoder
        self.publisher = None
        self.triggers = None

        self._setup_publisher()
        self._setup_triggers()

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
        self.triggers = setup_triggers(self._config, self.publisher, decoder=self.decoder)
        for trigger in self.triggers:
            trigger.start()

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
        except RuntimeError:
            logger.critical('Something went wrong!')
        except OSError:
            logger.critical('Something went wrong!')
        finally:
            logger.warning('Ending publication the gathering of granules...')
            for trigger in self.triggers:
                trigger.stop()
            self.publisher.stop()
