#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2014 - 2021 Pytroll developers
#
# Author(s):
#   Martin Raspaud <martin.raspaud@smhi.se>
#   Panu Lahtinen <panu.lahtinen@fmi.fi>
#   Adam Dybbroe <adam.dybbroe@smhi.se>
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

"""Gather messages of granules that cover geographic regions together and send them as a collection."""

import time
import logging
import logging.handlers
import os
import os.path

from configparser import RawConfigParser

from pytroll_collectors.geographic_gatherer import GeographicGatherer


def arg_parse():
    """Handle input arguments."""
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--log",
                        help="File to log to (defaults to stdout)",
                        default=None)
    parser.add_argument("-v", "--verbose", help="print debug messages too",
                        action="store_true")
    parser.add_argument("-c", "--config-item",
                        help="config item to use (all by default). Can be specified multiply times",
                        action="append")
    parser.add_argument("-p", "--publish-port", default=0, type=int,
                        help="Port to publish the messages on. Default: automatic")
    parser.add_argument("-n", "--nameservers",
                        help=("Connect publisher to given nameservers: "
                              "'-n localhost -n 123.456.789.0'. Default: localhost"),
                        action="append")
    parser.add_argument("config", help="config file to be used")

    return parser.parse_args()


def setup_logging(opts):
    """Setup logging."""
    handlers = []
    if opts.log:
        handlers.append(logging.handlers.TimedRotatingFileHandler(opts.log,
                                                                  "midnight",
                                                                  backupCount=7))
    handlers.append(logging.StreamHandler())

    if opts.verbose:
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO
    for handler in handlers:
        handler.setFormatter(logging.Formatter("[%(levelname)s: %(asctime)s :"
                                               " %(name)s] %(message)s",
                                               '%Y-%m-%d %H:%M:%S'))
        handler.setLevel(loglevel)
        logging.getLogger('').setLevel(loglevel)
        logging.getLogger('').addHandler(handler)

    logging.getLogger("posttroll").setLevel(logging.INFO)
    return logging.getLogger("gatherer")


def main():
    """Run the gatherer."""
    config = RawConfigParser()

    opts = arg_parse()
    config.read(opts.config)

    logger = setup_logging(opts)

    print("Setting timezone to UTC")
    os.environ["TZ"] = "UTC"
    time.tzset()

    if config is None:
        return

    granule_triggers = GeographicGatherer(config, opts)
    granule_triggers.run()

    logger.info("GeographicGatherer has stopped.")


if __name__ == '__main__':

    main()
