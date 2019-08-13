#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2013, 2014, 2015

# Author(s):

#   Joonas Karjalainen <joonas.karjalainen@fmi.fi>
#   Panu Lahtinen <panu.lahtinen@fmi.fi>
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

"""./trollstalker.py -c /path/to/trollstalker_config.ini -C noaa_hrpt
"""

import argparse
from pyinotify import WatchManager, ThreadedNotifier, ProcessEvent
import pyinotify
import sys
import time
from six.moves.configparser import RawConfigParser
import ConfigParser
import logging
import logging.config
import os
import os.path
import re
import datetime as dt
from collections import deque, OrderedDict

from posttroll.publisher import NoisyPublisher
from posttroll.message import Message
from trollsift import Parser, compose
from pytroll_collectors import helper_functions

LOGGER = logging.getLogger(__name__)


class EventHandler(ProcessEvent):

    """
    Event handler class for inotify.
     *topic* - topic of the published messages
     *posttroll_port* - port number to publish the messages on
     *filepattern* - filepattern for finding information from the filename

     *ref_filepattern* - REF file filepattern name
    """

    def __init__(self, topic, instrument, posttroll_port=0, filepattern=None,
                 aliases=None, tbus_orbit=False, history=0, granule_length=0,
                 custom_vars=None, nameservers=[], watchManager=None,
                 ref_filepattern=None):

        super(EventHandler, self).__init__()

        self._pub = NoisyPublisher("trollstalker", posttroll_port, topic,
                                   nameservers=nameservers)
        self.pub = self._pub.start()
        self.topic = topic
        self.info = OrderedDict()
        if filepattern is None:
            filepattern = '{filename}'
        self.file_parser = Parser(filepattern)
        self.instrument = instrument
        self.aliases = aliases
        self.custom_vars = custom_vars
        self.tbus_orbit = tbus_orbit
        self.granule_length = granule_length
        self._deque = deque([], history)
        self._watchManager = watchManager
        self._watched_dirs = dict()
        if ref_filepattern is not None:
            self.ref_file_parser = Parser(ref_filepattern)
        else:
            self.ref_file_parser = None

    def stop(self):
        '''Stop publisher.
        '''
        self._pub.stop()

    def __clean__(self):
        '''Clean instance attributes.
        '''
        self.info = OrderedDict()

    def process_IN_CLOSE_WRITE(self, event):
        """When a file is closed, process the associated event.
        """
        LOGGER.debug("trigger: IN_CLOSE_WRITE")
        self.process(event)

    def process_IN_CLOSE_NOWRITE(self, event):
        """When a nonwritable file is closed, process the associated event.
        """
        LOGGER.debug("trigger: IN_CREATE")
        self.process(event)

    def process_IN_MOVED_TO(self, event):
        """When a file is closed, process the associated event.
        """
        LOGGER.debug("trigger: IN_MOVED_TO")
        self.process(event)

    def process_IN_CREATE(self, event):
        """When a file is created, process the associated event.
        """
        LOGGER.debug("trigger: IN_CREATE")
        self.process(event)

    def process_IN_CLOSE_MODIFY(self, event):
        """When a file is modified and closed, process the associated event.
        """
        LOGGER.debug("trigger: IN_MODIFY")
        self.process(event)

    def process_IN_DELETE(self, event):
        """On delete."""
        if (event.mask & pyinotify.IN_ISDIR ):
            try:
                try:
                    self._watchManager.rm_watch(self._watched_dirs[event.pathname], quiet=False)
                except pyinotify.WatchManagerError:
                    #As the directory is deleted prior removing the watch will cause a error message
                    #from pyinotify. This is ok, so just pass the exception.
                    LOGGER.debug("Removed watch: {}".format(event.pathname))
                    pass
                finally:
                    del self._watched_dirs[event.pathname]
            except KeyError:
                LOGGER.warning("Dir {} not watched by inotify. Can not delete watch.".format(event.pathname))
        return

    def process(self, event):
        '''Process the event

            - Send a message if event is triggered on a satellite data file
            - If use of REF file is configured and event is triggered on a REF file:
                Read the referenced directory from the ref file and send messages for all data inside the
                referenced directory

                REF file internal format:
                [REF]
                SourcePath = /path/to/dataset
                FileName = ref_file_filename
        '''

        # New file created and closed
        if not event.dir:
            LOGGER.debug("processing %s", event.pathname)
            # parse information and create self.info OrderedDict{}
            self.parse_file_info(event)
            if len(self.info) > 0:
                # Check if this file has been recently dealt with
                if event.pathname not in self._deque:
                    self._deque.append(event.pathname)
                    message = self.create_message()
                    LOGGER.info("Publishing message %s", str(message))
                    self.pub.send(str(message))
                else:
                    LOGGER.info("Data has been published recently, skipping.")

            elif self.ref_file_parser is not None and self.ref_file_parser.validate(event.pathname):
                # It is a REF file
                # Generate messages for files found in referenced directory
                event_file = event.pathname
                LOGGER.info("Found REF file: %s", str(event_file))
                ref_file_content = parse_ref_file(event_file)
                if "REF" in ref_file_content and "sourcepath" in ref_file_content["REF"]:
                    scan_path = ref_file_content["REF"]["sourcepath"]
                    LOGGER.info("Generating messages for referenced path: %s", str(scan_path))
                    # use Filter to trigger file in referenced directory
                    if "filter" in ref_file_content["REF"]:
                        filter_ref = ref_file_content["REF"]["filter"]
                    else:
                        filter_ref = ".*"
                    for file in os.listdir(scan_path):
                        # scan all files in referenced folder and generate messages for all the files
                        if not os.path.isdir(file) and re.search(filter_ref, file):
                            event_new = event
                            event_new.name = file
                            event_new.path = scan_path
                            event_new.pathname = scan_path + "/" + file
                            self.parse_file_info(event_new)
                            if len(self.info) > 0:
                                # Check if this file has been recently dealt with
                                if event_new.pathname not in self._deque:
                                    self._deque.append(event_new.pathname)
                                    message = self.create_message()
                                    LOGGER.info("Publishing message %s", str(message))
                                    self.pub.send(str(message))
                                else:
                                    LOGGER.info("Data has been published recently, skipping.")
                            self.__clean__()
                else:
                    LOGGER.debug("Cannot extract information from REF file")
            self.__clean__()
        elif (event.mask & pyinotify.IN_ISDIR):
            tmask = (pyinotify.IN_CLOSE_WRITE | pyinotify.IN_MOVED_TO |
                     pyinotify.IN_CREATE | pyinotify.IN_DELETE)
            try:
                # If watched mask has been configured in cfg, use the configured one
                if self._watchManager._wmd is not None:
                    for wmd in self._watchManager._wmd:
                        tmask = self._watchManager._wmd[wmd].mask
                        break
                self._watched_dirs.update(self._watchManager.add_watch(event.pathname, tmask))
                LOGGER.debug("Added watch on dir: {}".format(event.pathname))
            except AttributeError:
                LOGGER.error("No watchmanager given. Can not add watch on {}".format(event.pathname))
                pass


    def create_message(self):
        """Create broadcasted message
        """
        return Message(self.topic, 'file', dict(self.info))

    def parse_file_info(self, event):
        '''Parse satellite and orbit information from the filename.
        Message is sent, if a matching filepattern is found.
        '''
        try:
            LOGGER.debug("filter: %s\t event: %s",
                         self.file_parser.fmt, event.pathname)
            pathname_join = os.path.basename(event.pathname)
            if 'origin_inotify_base_dir_skip_levels' in self.custom_vars:
                pathname_list = event.pathname.split('/')
                pathname_join = "/".join(pathname_list[int(self.custom_vars['origin_inotify_base_dir_skip_levels']):])
            else:
                LOGGER.debug("No origin_inotify_base_dir_skip_levels in self.custom_vars")

            self.info = OrderedDict()
            self.info.update(self.file_parser.parse(
                    pathname_join))
            LOGGER.debug("Extracted: %s", str(self.info))
        except ValueError:
            # Filename didn't match pattern, so empty the info dict
            LOGGER.info("Couldn't extract any usefull information")
            self.info = OrderedDict()
        else:
            self.info['uri'] = event.pathname
            self.info['uid'] = os.path.basename(event.pathname)
            self.info['sensor'] = self.instrument.split(',')
            LOGGER.debug("self.info['sensor']: " + str(self.info['sensor']))

            if self.tbus_orbit and "orbit_number" in self.info:
                LOGGER.info("Changing orbit number by -1!")
                self.info["orbit_number"] -= 1

            # replace values with corresponding aliases, if any are given
            if self.aliases:
                info = self.info.copy()
                for key in info:
                    if key in self.aliases:
                        self.info['orig_' + key] = self.info[key]
                        self.info[key] = self.aliases[key][str(self.info[key])]

            # add start_time and end_time if not present
            try:
                base_time = self.info["time"]
            except KeyError:
                try:
                    base_time = self.info["nominal_time"]
                except KeyError:
                    base_time = self.info["start_time"]
            if "start_time" not in self.info:
                self.info["start_time"] = base_time
            if "start_date" in self.info:
                self.info["start_time"] = \
                    dt.datetime.combine(self.info["start_date"].date(),
                                        self.info["start_time"].time())
                if "end_date" not in self.info:
                    self.info["end_date"] = self.info["start_date"]
                del self.info["start_date"]
            if "end_date" in self.info:
                self.info["end_time"] = \
                    dt.datetime.combine(self.info["end_date"].date(),
                                        self.info["end_time"].time())
                del self.info["end_date"]
            if "end_time" not in self.info and self.granule_length > 0:
                self.info["end_time"] = base_time + \
                    dt.timedelta(seconds=self.granule_length)

            if "end_time" in self.info:
                while self.info["start_time"] > self.info["end_time"]:
                    self.info["end_time"] += dt.timedelta(days=1)

            if self.custom_vars is not None:
                for var_name in self.custom_vars:
                    var_pattern = self.custom_vars[var_name]
                    var_val = None
                    if '%' in var_pattern:
                        var_val = helper_functions.create_aligned_datetime_var(
                            var_pattern, self.info)
                    if var_val is None:
                        var_val = compose(var_pattern, self.info)
                    self.info[var_name] = var_val


class NewThreadedNotifier(ThreadedNotifier):

    '''Threaded notifier class
    '''

    def stop(self, *args, **kwargs):
        self._default_proc_fun.stop()
        ThreadedNotifier.stop(self, *args, **kwargs)


def create_notifier(topic, instrument, posttroll_port, filepattern,
                    event_names, monitored_dirs, aliases=None,
                    tbus_orbit=False, history=0, granule_length=0,
                    custom_vars=None, nameservers=[], ref_filepattern=None):
    '''Create new notifier'''

    # Event handler observes the operations in defined folder
    manager = WatchManager()

    # Collect mask for events that are monitored
    if type(event_names) is not list:
        event_names = event_names.split(',')
    event_mask = 0
    for event in event_names:
        try:
            event_mask |= getattr(pyinotify, event)
        except AttributeError:
            LOGGER.warning('Event ' + event + ' not found in pyinotify')

    event_handler = EventHandler(topic, instrument,
                                 posttroll_port=posttroll_port,
                                 filepattern=filepattern,
                                 aliases=aliases,
                                 tbus_orbit=tbus_orbit,
                                 history=history,
                                 granule_length=granule_length,
                                 custom_vars=custom_vars,
                                 nameservers=nameservers,
                                 watchManager=manager,
                                 ref_filepattern = ref_filepattern)

    notifier = NewThreadedNotifier(manager, event_handler)

    # Add directories and event masks to watch manager
    for monitored_dir in monitored_dirs:
        manager.add_watch(monitored_dir, event_mask, rec=True)

    return notifier


def parse_vars(config):
    '''Parse custom variables from the config.

    Aliases are given in the config as:

    {'var_<name>': 'value'}

    where <name> is the name of the key which value will be
    added to metadata. <value> is a trollsift pattern.

    '''
    vars = OrderedDict()

    for key in config:
        if 'var_' in key:
            new_key = key.replace('var_', '')
            var = config[key]
            vars[new_key] = var
    return vars

def parse_ref_file(ref_filename):
    """ Parse REF file and return dictionary of the ref file content
    """
    reference_path = None
    ref_info = dict()
    try:
        ref_file = RawConfigParser()
        ref_file.read(ref_filename)
        for section in ref_file.sections():
            temp_dict = dict()
            for (key, val) in ref_file.items(section):
                temp_dict.update({key : val})
            ref_info.update({section : temp_dict})

    except ConfigParser.MissingSectionHeaderError:
        LOGGER.error("Wrong ref file format: " + str(ref_filename))
    except ConfigParser.ParsingError:
        LOGGER.error("Error parsing ref file: " + str(ref_filename))
    return ref_info


def main():
    '''Main(). Commandline parsing and stalker startup.'''

    print("Setting timezone to UTC")
    os.environ["TZ"] = "UTC"
    time.tzset()

    parser = argparse.ArgumentParser()

    parser.add_argument("-d", "--monitored_dirs", dest="monitored_dirs",
                        nargs='+',
                        type=str,
                        default=[],
                        help="Names of the monitored directories "
                        "separated by space")
    parser.add_argument("-p", "--posttroll_port", dest="posttroll_port",
                        default=0, type=int,
                        help="Local port where messages are published")
    parser.add_argument("-t", "--topic", dest="topic",
                        type=str,
                        default=None,
                        help="Topic of the sent messages")
    parser.add_argument("-c", "--configuration_file",
                        type=str,
                        help="Name of the config.ini configuration file")
    parser.add_argument("-C", "--config_item",
                        type=str,
                        help="Name of the configuration item to use")
    parser.add_argument("-e", "--event_names",
                        type=str, default=None,
                        help="Name of the pyinotify events to monitor")
    parser.add_argument("-f", "--filepattern",
                        type=str,
                        help="Filepattern used to parse "
                        "satellite/orbit/date/etc information")
    parser.add_argument("-i", "--instrument",
                        type=str, default=None,
                        help="Instrument name in the satellite")
    parser.add_argument("-n", "--nameservers",
                        type=str, default=None,
                        help="Posttroll nameservers to register own address,"
                        " otherwise multicasting is used")

    if len(sys.argv) <= 1:
        parser.print_help()
        sys.exit()
    else:
        args = parser.parse_args()

    # Parse commandline arguments.  If args are given, they override
    # the configuration file.

    # Check first commandline arguments
    monitored_dirs = args.monitored_dirs
    if monitored_dirs == '':
        monitored_dirs = None

    posttroll_port = args.posttroll_port
    topic = args.topic
    event_names = args.event_names
    instrument = args.instrument
    nameservers = args.nameservers

    filepattern = args.filepattern
    if args.filepattern == '':
        filepattern = None

    ref_filepattern = None

    if args.configuration_file is not None:
        config_fname = args.configuration_file

        if "template" in config_fname:
            print("Template file given as trollstalker logging config,"
                  " aborting!")
            sys.exit()

        config = RawConfigParser()
        config.read(config_fname)
        config = OrderedDict(config.items(args.config_item))
        config['name'] = args.configuration_file

        topic = topic or config['topic']
        monitored_dirs = monitored_dirs or config['directory'].split(",")
        filepattern = filepattern or config['filepattern']
        try:
            posttroll_port = posttroll_port or int(config['posttroll_port'])
        except (KeyError, ValueError):
            if posttroll_port is None:
                posttroll_port = 0
        try:
            filepattern = filepattern or config['filepattern']
        except KeyError:
            pass
        try:
            event_names = event_names or config['event_names']
        except KeyError:
            pass
        try:
            instrument = instrument or config['instruments']
        except KeyError:
            pass
        try:
            history = int(config['history'])
        except KeyError:
            history = 0

        try:
            nameservers = nameservers or config['nameservers']
        except KeyError:
            nameservers = []

        try:
            ref_filepattern = ref_filepattern or config['ref_filepattern']
        except KeyError:
            pass

        aliases = helper_functions.parse_aliases(config)
        tbus_orbit = bool(config.get("tbus_orbit", False))

        granule_length = float(config.get("granule", 0))

        custom_vars = parse_vars(config)

        try:
            log_config = config["stalker_log_config"]
        except KeyError:
            try:
                loglevel = getattr(logging, config["loglevel"])
                if loglevel == "":
                    raise AttributeError
            except AttributeError:
                loglevel = logging.DEBUG
            LOGGER.setLevel(loglevel)

            strhndl = logging.StreamHandler()
            strhndl.setLevel(loglevel)
            log_format = "[%(asctime)s %(levelname)-8s %(name)s] %(message)s"
            formatter = logging.Formatter(log_format)

            strhndl.setFormatter(formatter)
            LOGGER.addHandler(strhndl)
        else:
            logging.config.fileConfig(log_config)

    event_names = event_names or 'IN_CLOSE_WRITE,IN_MOVED_TO'

    LOGGER.debug("Logger started")

    if type(monitored_dirs) is not list:
        monitored_dirs = [monitored_dirs]

    if nameservers:
        nameservers = nameservers.split(',')
    else:
        nameservers = []

    # Start watching for new files
    notifier = create_notifier(topic, instrument, posttroll_port, filepattern,
                               event_names, monitored_dirs, aliases=aliases,
                               tbus_orbit=tbus_orbit, history=history,
                               granule_length=granule_length,
                               custom_vars=custom_vars,
                               nameservers=nameservers,
                               ref_filepattern=ref_filepattern)
    notifier.start()

    try:
        while True:
            time.sleep(6000000)
    except KeyboardInterrupt:
        LOGGER.info("Interupting TrollStalker")
    finally:
        notifier.stop()

if __name__ == "__main__":
    LOGGER = logging.getLogger("trollstalker")
    main()
