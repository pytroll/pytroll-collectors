#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2015, 2016 Panu Lahtinen

# Author(s): Panu Lahtinen

#   Panu Lahtinen <panu.lahtinen@fmi.fi>

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

import argparse
import os
import logging
import logging.handlers
import time
from ConfigParser import NoOptionError, RawConfigParser

from pytroll_collectors.segments import SegmentGatherer


def ini_to_dict(config, section):
    """Convert *section* of .ini *config* to dictionary."""
    conf = {}
    conf['posttroll'] = {}
    posttroll = conf['posttroll']
    posttroll['topics'] = config.get(section, 'topics').split()
    try:
        nameservers = config.get(section, 'nameserver')
        nameservers = nameservers.split()
    except (NoOptionError, ValueError):
        nameservers = None
    posttroll['nameservers'] = nameservers

    try:
        addresses = config.get(section, 'addresses')
        addresses = addresses.split()
    except (NoOptionError, ValueError):
        addresses = None
    posttroll['addresses'] = addresses

    try:
        publish_port = config.get(section, 'publish_port')
    except NoOptionError:
        publish_port = 0
    posttroll['publish_port'] = publish_port

    posttroll['publish_topic'] = config.get(section, "publish_topic")

    conf['patterns'] = {section: {}}
    patterns = conf['patterns'][section]
    patterns['pattern'] = config.get(section, 'pattern')
    patterns['critical_files'] = config.get(section, 'critical_files')
    patterns['wanted_files'] = config.get(section, 'wanted_files')
    patterns['all_files'] = config.get(section, 'all_files')
    patterns['required'] = False
    try:
        patterns['variable_tags'] = config.get(section, 'variable_tags')
    except NoOptionError:
        patterns['variable_tags'] = []

    try:
        conf['time_tolerance'] = config.getint(section, "time_tolerance")
    except NoOptionError:
        conf['time_tolerance'] = 30
    try:
        # Seconds
        conf['timeliness'] = config.getint(section, "timeliness")
    except (NoOptionError, ValueError):
        conf['timeliness'] = 1200

    try:
        conf['num_files_premature_publish'] = \
            config.getint(section, "num_files_premature_publish")
    except (NoOptionError, ValueError):
        conf['num_files_premature_publish'] = -1

    return conf


def arg_parse():
    '''Handle input arguments.
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--log",
                        help="File to log to (defaults to stdout)",
                        default=None)
    parser.add_argument("-v", "--verbose", help="print debug messages too",
                        action="store_true")
    parser.add_argument("-c", "--config", help="config file to be used")
    parser.add_argument("-C", "--config_item",
                        help="config item to use with .ini files")

    return parser.parse_args()


def main():
    '''Main. Parse cmdline, read config etc.'''

    args = arg_parse()

    if args.config_item:
        config = RawConfigParser()
        config.read(args.config)
        config = ini_to_dict(config, args.config_item)
    else:
        import yaml

        with open(args.config, 'r') as fid:
            config = yaml.load(fid)

    print "Setting timezone to UTC"
    os.environ["TZ"] = "UTC"
    time.tzset()

    handlers = []
    if args.log:
        handlers.append(
            logging.handlers.TimedRotatingFileHandler(args.log,
                                                      "midnight",
                                                      backupCount=7))

    handlers.append(logging.StreamHandler())

    if args.verbose:
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
    logger = logging.getLogger("segment_gatherer")

    gatherer = SegmentGatherer(config)
    gatherer.set_logger(logger)
    try:
        gatherer.run()
    finally:
        gatherer.stop()


if __name__ == "__main__":
    main()
