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

"""Gather segments.

Gather GEO stationary segments, or polar satellite granules for one timestep,
and send them in a bunch as a dataset.
"""

import datetime as dt
import logging
import logging.handlers
from six.moves.queue import Empty
from collections import OrderedDict
from six.moves.urllib.parse import urlparse

from posttroll import message, publisher
from posttroll.listener import ListenerContainer
from trollsift import Parser

SLOT_NOT_READY = 0
SLOT_NONCRITICAL_NOT_READY = 1
SLOT_READY = 2
SLOT_READY_BUT_WAIT_FOR_MORE = 3
SLOT_OBSOLETE_TIMEOUT = 4

DO_NOT_COPY_KEYS = ("uid", "uri", "channel_name", "segment", "sensor")
REMOVE_TAGS = {'path', 'segment'}


class SegmentGatherer(object):
    """Gatherer for geostationary satellite segments and multifile polar satellite granules."""

    _listener = None
    _publisher = None

    def __init__(self, config):
        """Initialize the segment gatherer."""
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
        # Floor the scene start time to the given full minutes
        self._group_by_minutes = config.get('group_by_minutes', None)

        # This get the 'keep_parsed_keys' valid for all patterns
        self._keep_parsed_keys = config.get('keep_parsed_keys', [])

        self.logger = logging.getLogger("segment_gatherer")
        self._loop = False
        self._providing_server = config.get('providing_server')

        # Convert check time into int minutes variables
        for key in self._patterns:
            if "start_time_pattern" in self._patterns[key]:
                time_conf = self._patterns[key]["start_time_pattern"]
                start_time_str = time_conf.get("start_time", "00:00")
                end_time_str = time_conf.get("end_time", "23:59")
                delta_time_str = time_conf.get("delta_time", "00:01")

                start_h, start_m = start_time_str.split(':')
                end_h, end_m = end_time_str.split(':')
                delta_h, delta_m = delta_time_str.split(':')
                interval = {}
                interval["start"] = (60 * int(start_h)) + int(start_m)
                interval["end"] = (60 * int(end_h)) + int(end_m)
                interval["delta"] = (60 * int(delta_h)) + int(delta_m)

                # Start-End time across midnight
                interval["midnight"] = False
                if interval["start"] > interval["end"]:
                    interval["end"] += 24 * 60
                    interval["midnight"] = True
                self._patterns[key]["_start_time_pattern"] = interval
                self.logger.info("Start Time pattern '%s' " +
                                 "filter start:%s end:%s delta:%s",
                                 key, start_time_str, end_time_str,
                                 delta_time_str)

    def _clear_data(self, time_slot):
        """Clear data."""
        if time_slot in self.slots:
            del self.slots[time_slot]

    def _init_data(self, mda):
        """Init wanted, all and critical files."""
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

        itm_str is formated like ':PRO,:EPI' or 'VIS006:8,VIS008:1-8,...'
        """
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
        """Update the timeout."""
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
        """Determine the overall status of the collection."""
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

    def _generate_publish_service_name(self):
        publish_service_name = "segment_gatherer"
        for key in sorted(self._patterns):
            publish_service_name += "_" + str(key)
        return publish_service_name

    def _setup_messaging(self):
        """Set up messaging."""
        self._subject = self._config['posttroll']['publish_topic']
        topics = self._config['posttroll'].get('topics')
        addresses = self._config['posttroll'].get('addresses')
        publish_port = self._config['posttroll'].get('publish_port', 0)
        nameservers = self._config['posttroll'].get('nameservers', [])
        services = self._config['posttroll'].get('services')
        self._listener = ListenerContainer(topics=topics, addresses=addresses, services=services)
        # Name each segment_gatherer with the section/patterns name.
        # This way the user can subscribe to a specific segment_gatherer service instead of all.
        publish_service_name = self._generate_publish_service_name()
        self._publisher = publisher.NoisyPublisher(publish_service_name,
                                                   port=publish_port,
                                                   nameservers=nameservers)
        self._publisher.start()

    def run(self):
        """Run SegmentGatherer."""
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
            except Empty:
                continue

            if msg.type == "file":
                # If providing server is configured skip message if not from providing server
                if self._providing_server and self._providing_server != msg.host:
                    continue
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
        """Process message."""
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
        mda = self._floor_time(mda, key)

        # Check if each pattern contains a seperate 'keep_parsed_keys'
        local_keep_parsed_keys = self._patterns[key].get('keep_parsed_keys', [])
        metadata = copy_metadata(mda, msg,
                                 keep_parsed_keys=self._keep_parsed_keys,
                                 local_keep_parsed_keys=local_keep_parsed_keys)

        # Check if time of the raw is in scheduled range
        if "_start_time_pattern" in self._patterns[key]:
            schedule_ok = self.check_schedule_time(
                self._patterns[key]["_start_time_pattern"],
                metadata[self.time_name])
            if not schedule_ok:
                self.logger.info("Hour pattern '%s' skip: %s" +
                                 " for start_time: %s:%s",
                                 key, msg.data["uid"],
                                 metadata[self.time_name].hour,
                                 metadata[self.time_name].minute)
                return

        time_slot = self._find_time_slot(metadata[self.time_name])

        # Init metadata etc if this is the first file
        if time_slot not in self.slots:
            self._init_data(metadata)

        # Check if this file has been received already
        self.add_file(time_slot, key, mda, msg.data)

    def _floor_time(self, mda, key=None):
        """Floor time to full minutes."""
        # This is the 'group_by_minutes' for all patterns
        group_by_minutes = self._group_by_minutes

        # Check if 'group_by_minutes' is given in the \key\ pattern
        if key is not None and 'group_by_minutes' in self._patterns[key]:
            group_by_minutes = self._patterns[key]['group_by_minutes']
        elif self._group_by_minutes is None:
            return mda

        start_time = mda[self.time_name]
        mins = start_time.minute
        fl_mins = int(mins / group_by_minutes) * group_by_minutes
        start_time = dt.datetime(start_time.year, start_time.month,
                                 start_time.day, start_time.hour, fl_mins, 0)
        mda[self.time_name] = start_time

        return mda

    def add_file(self, time_slot, key, mda, msg_data):
        """Add file to the correct filelist."""
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
        """Get the keys from a filename."""
        for key in self._parsers:
            try:
                _ = self._parsers[key].parse(uid)
                return key
            except ValueError:
                pass

    def _find_time_slot(self, time_obj):
        """Find time slot and return the slot as a string.

        If no slots are close enough, return *str(time_obj)*
        """
        for slot in self.slots:
            time_slot = self.slots[slot]['metadata'][self.time_name]
            time_diff = time_obj - time_slot
            if abs(time_diff.total_seconds()) < self._time_tolerance:
                self.logger.debug("Found existing time slot, using that")
                return str(time_slot)

        return str(time_obj)

    def check_schedule_time(self, check_time, raw_start_time):
        """Check if raw time is inside configured interval."""
        time_ok = False

        # Convert check time into int variables
        raw_time = (60 * raw_start_time.hour) + raw_start_time.minute
        if check_time["midnight"] and raw_time < check_time["start"]:
            raw_time += 24 * 60

        # Check start and end time
        if raw_time >= check_time["start"] and raw_time <= check_time["end"]:
            # Raw time in range, check interval
            if ((raw_time - check_time["start"]) % check_time["delta"]) == 0:
                time_ok = True

        return time_ok


def _copy_without_ignore_items(the_dict, ignored_keys='ignore'):
    """Get a copy of *the_dict* without entries having substring 'ignore' in key."""
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
        nameservers = config.get(section, 'nameservers')
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
        services = config.get(section, 'services')
        services = services.split()
    except (NoOptionError, ValueError):
        services = ""
    posttroll['services'] = services

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

    try:
        conf['group_by_minutes'] = config.getint(section, 'group_by_minutes')
    except (NoOptionError, ValueError):
        pass

    try:
        kps = config.get(section, 'keep_parsed_keys')
        conf['keep_parsed_keys'] = kps.split()
    except NoOptionError:
        pass

    try:
        conf['providing_server'] = config.get(section, "providing_server")
    except (NoOptionError, ValueError):
        conf['providing_server'] = None

    return conf


def copy_metadata(mda, msg, keep_parsed_keys=None, local_keep_parsed_keys=None):
    """Copy metada from filename and message to a combined dictionary."""
    if keep_parsed_keys is None:
        keep_parsed_keys = []
    if local_keep_parsed_keys is None:
        local_keep_parsed_keys = []
    metadata = {}
    # Use values parsed from the filename as basis
    for key in mda:
        if key not in DO_NOT_COPY_KEYS:
            metadata[key] = mda[key]

    # Update with data given in the message
    for key in msg.data:
        # If time name is given, do not overwrite it
        if key not in DO_NOT_COPY_KEYS and key not in keep_parsed_keys and key not in local_keep_parsed_keys:
            metadata[key] = msg.data[key]

    return metadata
