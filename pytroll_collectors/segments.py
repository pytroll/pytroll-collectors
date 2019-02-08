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

"""Gather GEO stationary segments, or polar satellite granules for one
timestep, and send them in a bunch as a dataset.
"""

import datetime as dt
import logging
import logging.handlers
import os.path
from six.moves.queue import Empty as queue_empty
import time
from collections import OrderedDict
from six.moves.urllib.parse import urlparse, urlunparse

from posttroll import message, publisher
from posttroll.listener import ListenerContainer
from trollsift import Parser, compose

SLOT_NOT_READY = 0
SLOT_NONCRITICAL_NOT_READY = 1
SLOT_READY = 2
SLOT_READY_BUT_WAIT_FOR_MORE = 3
SLOT_OBSOLETE_TIMEOUT = 4

DO_NOT_COPY_KEYS = ("uid", "uri", "channel_name", "segment", "sensor")
REMOVE_TAGS = {'path', 'segment'}


class SegmentGatherer(object):

    """Gatherer for geostationary satellite segments and multifile polar
    satellite granules."""

    _listener = None
    _publisher = None

    def __init__(self, config):
        self._config = config
        self._subject = None
        self._patterns = config['patterns']

        self._time_tolerance = config.get("time_tolerance", 30)
        self._timeliness = dt.timedelta(seconds=config.get("timeliness", 1200))

        self._num_files_premature_publish = \
            config.get("num_files_premature_publish", -1)

        self.slots = OrderedDict()

        self._parsers = {key: Parser(self._patterns[key]['pattern']) for
                         key in self._patterns}

        self.time_name = config.get('time_name', 'start_time')

        self.logger = logging.getLogger("segment_gatherer")
        self._loop = False

    def _clear_data(self, time_slot):
        """Clear data."""
        if time_slot in self.slots:
            del self.slots[time_slot]

    def _init_data(self, mda):
        """Init wanted, all and critical files"""
        # Init metadata struct
        metadata = mda.copy()

        time_slot = str(metadata[self.time_name])
        self.logger.debug("Adding new slot: %s", time_slot)
        self.slots[time_slot] = {}
        self.slots[time_slot]['metadata'] = metadata.copy()
        self.slots[time_slot]['timeout'] = None

        # Critical files that are required, otherwise production will fail.
        # If there are no critical files, empty set([]) is used.
        patterns = self._config['patterns']
        if len(patterns) == 1:
            self.slots[time_slot]['metadata']['dataset'] = []
        else:
            self.slots[time_slot]['metadata']['collection'] = {}
        for key in patterns:
            if len(patterns) > 1:
                self.slots[time_slot]['metadata']['collection'][key] = \
                    {'dataset': [], 'sensor': []}
            self.slots[time_slot][key] = {}
            slot = self.slots[time_slot][key]
            is_critical_set = patterns[key].get("is_critical_set", False)
            slot['is_critical_set'] = is_critical_set
            slot['critical_files'] = set([])
            slot['wanted_files'] = set([])
            slot['all_files'] = set([])
            slot['received_files'] = set([])
            slot['delayed_files'] = dict()
            slot['missing_files'] = set([])
            slot['files_till_premature_publish'] = \
                self._num_files_premature_publish

            critical_segments = patterns[key].get("critical_files", None)
            fname_set = self._compose_filenames(key, time_slot,
                                                critical_segments)
            if critical_segments:
                slot['critical_files'].update(fname_set)

            else:
                if is_critical_set:
                    # If critical segments are not defined, but the
                    # file based on this pattern is required, add it
                    # to critical files
                    slot['critical_files'].update(fname_set)

                # In any case add it to the wanted and all files
                slot['wanted_files'].update(fname_set)
                slot['all_files'].update(fname_set)

            # These segments are wanted, but not critical to production
            wanted_segments = patterns[key].get("wanted_files", None)
            slot['wanted_files'].update(
                self._compose_filenames(key, time_slot, wanted_segments))

            # Name of all the files
            all_segments = patterns[key].get("all_files", None)
            slot['all_files'].update(
                self._compose_filenames(key, time_slot, all_segments))

    def _compose_filenames(self, key, time_slot, itm_str):
        """Compose filename set()s based on a pattern and item string.
        itm_str is formated like ':PRO,:EPI' or 'VIS006:8,VIS008:1-8,...'"""

        # Empty set
        result = set()

        # Handle missing itm_str
        if itm_str in (None, ''):
            itm_str = ':'

        # Get copy of metadata
        meta = self.slots[time_slot]['metadata'].copy()

        # Replace variable tags (such as processing time) with
        # wildcards, as these can't be forecasted.
        var_tags = self._config['patterns'][key].get('variable_tags', [])
        meta = _copy_without_ignore_items(meta,
                                          ignored_keys=var_tags)

        parser = self._parsers[key]

        for itm in itm_str.split(','):
            channel_name, segments = itm.split(':')
            if channel_name == '' and segments == '':
                # If the filename pattern has no segments/channels,
                # add the "plain" globified filename to the filename
                # set
                if ('channel_name' not in parser.fmt and
                    'segment' not in parser.fmt):
                    result.add(parser.globify(meta))
                continue
            segments = segments.split('-')
            if len(segments) > 1:
                format_string = '%d'
                if len(segments[0]) > 1 and segments[0][0] == '0':
                    format_string = '%0' + str(len(segments[0])) + 'd'
                segments = [format_string % i
                            for i in range(int(segments[0]),
                                           int(segments[-1]) + 1)]
            meta['channel_name'] = channel_name
            for seg in segments:
                meta['segment'] = seg
                fname = parser.globify(meta)

                result.add(fname)

        return result

    def _publish(self, time_slot, missing_files_check=True):
        """Publish file dataset and reinitialize gatherer."""

        data = self.slots[time_slot]

        # Diagnostic logging about delayed ...
        delayed_files = {}
        for key in self._parsers:
            delayed_files.update(data[key]['delayed_files'])
        if len(delayed_files) > 0:
            file_str = ''
            for key in delayed_files:
                file_str += "%s %f seconds, " % (key, delayed_files[key])
            self.logger.warning("Files received late: %s",
                                file_str.strip(', '))

        # ... and missing files
        if missing_files_check:
            missing_files = set([])
            for key in self._parsers:
                missing_files = data[key]['all_files'].difference(
                    data[key]['received_files'])
            if len(missing_files) > 0:
                self.logger.warning("Missing files: %s",
                                    ', '.join(missing_files))

        # Remove tags that are not necessary for datasets
        for tag in REMOVE_TAGS:
            try:
                del data['metadata'][tag]
            except KeyError:
                pass

        if len(self._parsers) == 1:
            msg = message.Message(self._subject, "dataset", data['metadata'])
        else:
            msg = message.Message(self._subject, "collection", data['metadata'])
        self.logger.info("Sending: %s", str(msg))
        self._publisher.send(str(msg))

        # self._clear_data(time_slot)

    def set_logger(self, logger):
        """Set logger."""
        self.logger = logger

    def update_timeout(self, time_slot):
        timeout = dt.datetime.utcnow() + self._timeliness
        self.slots[time_slot]['timeout'] = timeout
        self.logger.info("Setting timeout to %s for slot %s.",
                         str(timeout), time_slot)

    def slot_ready(self, time_slot):
        """Determine if slot is ready to be published."""
        slot = self.slots[time_slot]

        if slot['timeout'] is None:
            self.update_timeout(time_slot)
            return SLOT_NOT_READY

        status = {}
        num_files = {}
        for key in self._parsers:
            # Default
            status[key] = SLOT_NOT_READY
            if not slot[key]['is_critical_set']:
                status[key] = SLOT_NONCRITICAL_NOT_READY

            wanted_and_critical_files = slot[key][
                'wanted_files'].union(slot[key]['critical_files'])
            num_wanted_and_critical = len(
                wanted_and_critical_files & slot[key]['received_files'])

            num_files[key] = num_wanted_and_critical

            if num_wanted_and_critical == \
               slot[key]['files_till_premature_publish']:
                slot[key]['files_till_premature_publish'] = -1
                status[key] = SLOT_READY_BUT_WAIT_FOR_MORE

            if wanted_and_critical_files.issubset(
                    slot[key]['received_files']):
                status[key] = SLOT_READY

        # Determine overall status
        return self.get_collection_status(status, slot['timeout'], time_slot)

    def get_collection_status(self, status, timeout, time_slot):
        """Determine the overall status of the collection"""
        if len(status) == 0:
            return SLOT_NOT_READY

        status_values = list(status.values())

        if all([val == SLOT_READY for val in status_values]):
            self.logger.info("Required files received "
                             "for slot %s.", time_slot)
            return SLOT_READY

        if dt.datetime.utcnow() > timeout:
            if (SLOT_NONCRITICAL_NOT_READY in status_values and
                (SLOT_READY in status_values or
                    SLOT_READY_BUT_WAIT_FOR_MORE in status_values)):
                return SLOT_READY
            elif (SLOT_READY_BUT_WAIT_FOR_MORE in status_values and
                  SLOT_NOT_READY not in status_values):
                return SLOT_READY
            elif all([val == SLOT_NONCRITICAL_NOT_READY for val in
                      status_values]):
                for key in status.keys():
                    if len(self.slots[time_slot][key]['received_files']) > 0:
                        return SLOT_READY
                return SLOT_OBSOLETE_TIMEOUT
            else:
                self.logger.warning("Timeout occured and required files "
                                    "were not present, data discarded for "
                                    "slot %s.",
                                    time_slot)
                return SLOT_OBSOLETE_TIMEOUT

        if SLOT_NOT_READY in status_values:
            return SLOT_NOT_READY
        if SLOT_NONCRITICAL_NOT_READY in status_values:
            return SLOT_NONCRITICAL_NOT_READY
        if SLOT_READY_BUT_WAIT_FOR_MORE in status_values:
            return SLOT_READY_BUT_WAIT_FOR_MORE

    def _setup_messaging(self):
        """Setup messaging"""
        self._subject = self._config['posttroll']['publish_topic']
        topics = self._config['posttroll'].get('topics')
        addresses = self._config['posttroll'].get('addresses')
        publish_port = self._config['posttroll'].get('publish_port', 0)
        nameservers = self._config['posttroll'].get('nameservers', [])
        self._listener = ListenerContainer(topics=topics, addresses=addresses)
        self._publisher = publisher.NoisyPublisher("segment_gatherer",
                                                   port=publish_port,
                                                   nameservers=nameservers)
        self._publisher.start()

    def run(self):
        """Run SegmentGatherer"""
        self._setup_messaging()

        self._loop = True
        while self._loop:
            # Check if there are slots ready for publication
            slots = self.slots.copy()
            for slot in slots:
                slot = str(slot)
                status = self.slot_ready(slot)
                if status == SLOT_READY:
                    # Collection ready, publish and remove
                    self._publish(slot)
                    self._clear_data(slot)
                if status == SLOT_READY_BUT_WAIT_FOR_MORE:
                    # Collection ready, publish and but wait for more
                    self._publish(slot, missing_files_check=False)
                elif status == SLOT_OBSOLETE_TIMEOUT:
                    # Collection unfinished and obslote, discard
                    self._clear_data(slot)
                else:
                    # Collection unfinished, wait for more data
                    pass

            # Check listener for new messages
            msg = None
            try:
                msg = self._listener.output_queue.get(True, 1)
            except AttributeError:
                msg = self._listener.queue.get(True, 1)
            except KeyboardInterrupt:
                self.stop()
                continue
            except queue_empty:
                continue

            if msg.type == "file":
                self.logger.info("New message received: %s", str(msg))
                self.process(msg)

    def stop(self):
        """Stop gatherer."""
        self.logger.info("Stopping gatherer.")
        self._loop = False
        if self._listener is not None:
            if self._listener.thread is not None:
                self._listener.stop()
        if self._publisher is not None:
            self._publisher.stop()

    def process(self, msg):
        """Process message"""
        mda = None

        try:
            uid = msg.data['uid']
        except KeyError:
            self.logger.debug("Ignoring: %s", str(msg))
            return

        # Find the correct parser for this file
        key = self.key_from_fname(uid)
        if key is None:
            self.logger.debug("Unknown file, skipping.")
            return

        parser = self._parsers[key]
        mda = parser.parse(msg.data["uid"])

        metadata = copy_metadata(mda, msg)

        time_slot = self._find_time_slot(metadata["start_time"])

        # Init metadata etc if this is the first file
        if time_slot not in self.slots:
            self._init_data(metadata)

        # Check if this file has been received already
        self.add_file(time_slot, key, mda, msg.data)

    def add_file(self, time_slot, key, mda, msg_data):
        """Add file to the correct filelist"""
        uri = urlparse(msg_data['uri']).path
        uid = msg_data['uid']
        slot = self.slots[time_slot][key]
        meta = self.slots[time_slot]['metadata']

        # Replace variable tags (such as processing time) with
        # wildcards, as these can't be forecasted.
        ignored_keys = \
            self._config['patterns'][key].get('variable_tags', [])
        mda = _copy_without_ignore_items(mda,
                                         ignored_keys=ignored_keys)

        mask = self._parsers[key].globify(mda)
        if mask in slot['received_files']:
            self.logger.debug("File already received")
            return
        if mask not in slot['all_files']:
            self.logger.debug("%s not in %s", mask, slot['all_files'])
            return

        # self.update_timeout(time_slot)
        timeout = self.slots[time_slot]['timeout']

        # Add uid and uri
        if len(self._patterns) == 1:
            meta['dataset'].append({'uri': uri, 'uid': uid})
            sensors = meta.get('sensor', [])
        else:
            meta['collection'][key]['dataset'].append({'uri': uri, 'uid': uid})
            sensors = meta['collection'][key].get('sensor', [])

        # Collect all sensors, not only the latest
        if not isinstance(msg_data["sensor"], (tuple, list, set)):
            msg_data["sensor"] = [msg_data["sensor"]]
        if not isinstance(sensors, list):
            sensors = [sensors]
        for sensor in msg_data["sensor"]:
            if sensor not in sensors:
                sensors.append(sensor)
        meta['sensor'] = sensors

        # If critical files have been received but the slot is
        # not complete, add the file to list of delayed files
        if len(slot['critical_files']) > 0 and \
           slot['critical_files'].issubset(slot['received_files']):
            delay = dt.datetime.utcnow() - (timeout - self._timeliness)
            if delay.total_seconds() > 0:
                slot['delayed_files'][uid] = delay.total_seconds()

        # Add to received files
        slot['received_files'].add(mask)
        self.logger.info("%s processed", uid)

    def key_from_fname(self, uid):
        """"""
        for key in self._parsers:
            try:
                _ = self._parsers[key].parse(uid)
                return key
            except ValueError:
                pass

    def _find_time_slot(self, time_obj):
        """Find time slot and return the slot as a string.  If no slots are
        close enough, return *str(time_obj)*"""
        for slot in self.slots:
            time_slot = self.slots[slot]['metadata'][self.time_name]
            time_diff = time_obj - time_slot
            if abs(time_diff.total_seconds()) < self._time_tolerance:
                self.logger.debug("Found existing time slot, using that")
                return str(time_slot)

        return str(time_obj)


def _copy_without_ignore_items(the_dict, ignored_keys='ignore'):
    """
    get a copy of *the_dict* without entries having substring
    'ignore' in key
    """
    if not isinstance(ignored_keys, (list, tuple, set)):
        ignored_keys = [ignored_keys, ]
    new_dict = {}
    for (key, val) in list(the_dict.items()):
        if key not in ignored_keys:
            new_dict[key] = val
    return new_dict


def ini_to_dict(fname, section):
    """Convert *section* of .ini *config* to dictionary."""
    from six.moves.configparser import RawConfigParser, NoOptionError

    config = RawConfigParser()
    config.read(fname)

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
    patterns['is_critical_set'] = False
    try:
        patterns['variable_tags'] = config.get(section,
                                               'variable_tags').split(',')
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


def copy_metadata(mda, msg):
    """Copy metada from filename and message to a combined dictionary"""
    metadata = {}
    # Use values parsed from the filename as basis
    for key in mda:
        if key not in DO_NOT_COPY_KEYS:
            metadata[key] = mda[key]

    # Update with data given in the message
    for key in msg.data:
        if key not in DO_NOT_COPY_KEYS:
            metadata[key] = msg.data[key]

    return metadata
