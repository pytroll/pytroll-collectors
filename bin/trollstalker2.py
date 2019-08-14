#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2013 - 2019 PyTroll Community

# Author(s):

#   Joonas Karjalainen <joonas.karjalainen@fmi.fi>
#   Panu Lahtinen <panu.lahtinen@fmi.fi>
#   Martin Raspaud <martin.raspaud@smhi.se>
#   Adam Dybbroe <adam.dybbroe@smhi.se

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

"""./trollstalker.py -c /path/to/trollstalker_config.ini -C noaa_hrpt
"""

import argparse
import sys
import time
from six.moves.configparser import RawConfigParser
import logging
import logging.config
import os.path
import datetime as dt

from posttroll.publisher import NoisyPublisher
from posttroll.message import Message
from trollsift import Parser

LOGGER = logging.getLogger(__name__)

try:
    # Python 2
    str_or_unicode = (str, unicode)
except NameError:
    # Pyton 3
    str_or_unicode = (str, bytes)

from pytroll_collectors.trigger import AbstractWatchDogProcessor


class FilePublisher(AbstractWatchDogProcessor):

    def __init__(self, config):
        self.config = config.copy()
        if isinstance(config["filepattern"], str_or_unicode):
            self.config["filepattern"] = [self.config["filepattern"]]

        self.parsers = [Parser(filepattern)
                        for filepattern in self.config["filepattern"]]

        self.aliases = parse_aliases(config)

        self.topic = self.config["topic"]
        self.tbus_orbit = self.config.get("tbus_orbit", False)
        LOGGER.debug("Looking for: %s", str(
            [parser.globify() for parser in self.parsers]))
        AbstractWatchDogProcessor.__init__(self,
                                           [parser.globify()
                                            for parser in self.parsers],
                                           config.get("watcher",
                                                      "Observer"))

        self._pub = NoisyPublisher("trollstalker",
                                   int(self.config["posttroll_port"]),
                                   self.config["topic"])
        self.pub = None

        obsolete_keys = ["topic", "filepattern", "tbus_orbit",
                         "posttroll_port", "watch", "config_item", "configuration_file"]

        for key in list(self.config.keys()):
            if key.startswith("alias_") or key in obsolete_keys:
                del self.config[key]

    def start(self):
        AbstractWatchDogProcessor.start(self)
        self.pub = self._pub.start()

    def stop(self):
        self._pub.stop()
        AbstractWatchDogProcessor.stop(self)

    def process(self, pathname):
        '''Process the event'''
        # New file created and closed
        LOGGER.debug("processing %s", pathname)
        # parse information and create self.info dict{}
        metadata = self.config.copy()
        success = False
        for parser in self.parsers:
            try:
                metadata.update(parser.parse(pathname))
                success = True
                break
            except ValueError:
                pass
            if not success:
                LOGGER.warning("Could not find a matching pattern for %s",
                               pathname)

        metadata['uri'] = pathname
        metadata['uid'] = os.path.basename(pathname)

        if self.tbus_orbit and "orbit_number" in metadata:
            LOGGER.info("Changing orbit number by -1!")
            metadata["orbit_number"] -= 1

        # replace values with corresponding aliases, if any are given
        if self.aliases:
            for key in metadata:
                if key in self.aliases:
                    metadata[key] = self.aliases[key][str(metadata[key])]

        message = Message(self.topic, 'file', metadata)
        LOGGER.info("Publishing message %s" % str(message))
        self.pub.send(str(message))


def parse_aliases(config):
    '''Parse aliases from the config.

    Aliases are given in the config as:

    {'alias_<name>': 'value:alias'}, or
    {'alias_<name>': 'value1:alias1|value2:alias2'},

    where <name> is the name of the key which value will be
    replaced. The later form is there to support several possible
    substitutions (eg. '2' -> '9' and '3' -> '10' in the case of MSG).

    '''
    aliases = {}

    for key, alias in config.items():
        if key.startswith('alias_'):
            new_key = key.replace('alias_', '')
            if '|' in alias or ':' in alias:
                alias = dict(part.split(":") for part in alias.split("|"))
            aliases[new_key] = alias
    return aliases


def main():
    '''Main(). Commandline parsing and stalker startup.'''

    parser = argparse.ArgumentParser()

    parser.add_argument("-p", "--posttroll_port", dest="posttroll_port",
                        help="Local port where messages are published")
    parser.add_argument("-t", "--topic", dest="topic",
                        help="Topic of the sent messages")
    parser.add_argument("-c", "--configuration_file",
                        help="Name of the config.ini configuration file")
    parser.add_argument("-C", "--config_item",
                        help="Name of the configuration item to use")
    parser.add_argument("-e", "--event_names",
                        help="Name of the events to monitor")
    parser.add_argument("-f", "--filepattern",
                        help="Filepath pattern used to parse "
                        "satellite/orbit/date/etc information")
    parser.add_argument("-i", "--instrument",
                        help="Instrument name in the satellite")

    if len(sys.argv) <= 1:
        parser.print_help()
        sys.exit()
    else:
        args = parser.parse_args()

    # Parse commandline arguments.  If args are given, they override
    # the configuration file.

    args_dict = vars(args)
    args_dict = {k: args_dict[k]
                 for k in args_dict if args_dict[k] != None}

    config = {}

    if args.configuration_file is not None:
        config_fname = args.configuration_file

        if "template" in config_fname:
            print("Template file given as trollstalker logging config,"
                  " aborting!")
            sys.exit()

        cparser = RawConfigParser()
        cparser.read(config_fname)
        config = dict(cparser.items(args.config_item, vars=args_dict))

    config.update(args_dict)

    config.update({k: config[k].split(",")
                   for k in config if "," in config[k]})

    config.setdefault("posttroll_port", "0")

    try:
        log_config = config["stalker_log_config"]
    except KeyError:
        try:
            loglevel = getattr(logging, config.get("loglevel", "DEBUG"))
            if loglevel == "":
                raise AttributeError
        except AttributeError:
            loglevel = logging.DEBUG

        LOGGER.setLevel(loglevel)
        rootlogger = logging.getLogger("")
        rootlogger.setLevel(loglevel)
        strhndl = logging.StreamHandler()
        strhndl.setLevel(loglevel)
        log_format = "[%(asctime)s %(levelname)-8s %(name)s] %(message)s"
        formatter = logging.Formatter(log_format)

        strhndl.setFormatter(formatter)
        rootlogger.addHandler(strhndl)
    else:
        logging.config.fileConfig(log_config)

    LOGGER.debug("Logger started")

    # Start watching for new files
    notifier = FilePublisher(config)
    notifier.start()

    try:
        while True:
            time.sleep(6000000)
    except KeyboardInterrupt:
        LOGGER.info("Interrupting TrollStalker")
    finally:
        notifier.stop()


if __name__ == "__main__":
    # LOGGER = logging.getLogger("trollstalker")
    LOGGER = logging.getLogger("trollstalker")
    main()
