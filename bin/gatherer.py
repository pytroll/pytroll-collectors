#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014 - 2018 PyTroll Community

# Author(s): Martin Raspaud
#            Panu Lahtinen
#            Adam Dybbroe

#   Martin Raspaud <martin.raspaud@smhi.se>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Gather granule messages to send them in a bunch."""

import time
import logging
import logging.handlers
import os
import os.path

from six.moves.configparser import RawConfigParser

from pytroll_collectors.trigger import get_metadata, setup_triggers
from posttroll import publisher


logger = logging.getLogger(__name__)


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


def main():
    """Run the gatherer."""
    config = RawConfigParser()
    global logger

    opts = arg_parse()
    config.read(opts.config)

    print("Setting timezone to UTC")
    os.environ["TZ"] = "UTC"
    time.tzset()

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
    logger = logging.getLogger("gatherer")

    if opts.config_item:
        for section in opts.config_item:
            if section not in config.sections():
                logger.warning(
                    "No config item called %s found in config file.", section)
        for section in config.sections():
            if section not in opts.config_item:
                config.remove_section(section)
        if len(config.sections()) == 0:
            logger.error("No valid config item provided")
            return
        publisher_name = "gatherer_" + "_".join(opts.config_item)
    else:
        publisher_name = "gatherer"

    publish_port = opts.publish_port
    publisher_nameservers = opts.nameservers

    pub = publisher.NoisyPublisher(publisher_name, port=publish_port,
                                   nameservers=publisher_nameservers)

    granule_triggers = setup_triggers(config, pub, decoder=get_metadata)

    pub.start()

    for granule_trigger in granule_triggers:
        granule_trigger.start()
    try:
        while True:
            time.sleep(1)
            for granule_trigger in granule_triggers:
                if not granule_trigger.is_alive():
                    raise RuntimeError
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except RuntimeError:
        logger.critical('Something went wrong!')
    except OSError:
        logger.critical('Something went wrong!')
    finally:
        logger.warning('Ending publication the gathering of granules...')
        for granule_trigger in granule_triggers:
            granule_trigger.stop()
        pub.stop()


if __name__ == '__main__':

    main()
