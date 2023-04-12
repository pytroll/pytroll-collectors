#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2015 - 2021 Pytroll developers
#
# Author(s): Panu Lahtinen
#
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

"""Gather segments.

Gather posttroll messages corresponding to individual files that belong
to the same measurement start time.  This may be different instruments
on polar satellite granules, different channels, or geostationary image
segments.

When all are present, send a single posttroll message containing all
the segments in a bunch.

To gather multiple files with different start times, the gatherer
module/script should be used.  This is the case if a polar satellite
reception system produces multiple files for the same overpass.
"""

import datetime as dt
import logging.handlers
from abc import ABCMeta, abstractmethod
from collections import OrderedDict
from enum import Enum
import os

import trollsift
from posttroll import message as pmessage
from posttroll.listener import ListenerContainer
from queue import Empty
from urllib.parse import urlparse, urlunparse

from pytroll_collectors.utils import check_nameserver_options
from pytroll_collectors.utils import create_started_publisher_from_config
from pytroll_collectors.utils import create_publisher_config_dict

logger = logging.getLogger("segment_gatherer")


class Status(Enum):
    """Enumeration of slot statuses."""

    SLOT_NOT_READY = 0
    SLOT_NONCRITICAL_NOT_READY = 1
    SLOT_READY = 2
    SLOT_READY_BUT_WAIT_FOR_MORE = 3
    SLOT_OBSOLETE_TIMEOUT = 4


DO_NOT_COPY_KEYS = ("uid", "uri", "channel_name", "segment", "sensor")
REMOVE_TAGS = {'path', 'segment'}


class Parser(metaclass=ABCMeta):
    """Abstract class for parsing messages."""

    @abstractmethod
    def globify(self, mda):
        """Globify the message."""

    @abstractmethod
    def parse(self, msg):
        """Parse the message."""

    @abstractmethod
    def matches(self, msg):
        """Check if the message matches."""


class UIDParser(Parser):
    """Wrapper around trollsifts parser."""

    def __init__(self, config, pattern_name):
        """Set up the UID parser."""
        self._ts_parser = trollsift.Parser(config['pattern'])
        self.variable_tags = config.get('variable_tags', [])
        self._pattern_name = pattern_name

    def globify(self, mda):
        """Globify for the metadata."""
        return self._ts_parser.globify(mda)

    def parse(self, metadata):
        """Parse the uid of the message."""
        uid = metadata['uid']
        return self._ts_parser.parse(uid)

    def matches(self, msg):
        """Check that the message matches."""
        uid = msg.data['uid']
        return self._ts_parser.validate(uid)

    @property
    def fmt(self):
        """Get the keys of the uid pattern."""
        return self._ts_parser.fmt

    def uid(self, metadata):
        """Get the uid of the message."""
        return metadata['uid']


class MessageParser(Parser):
    """Use message to get metadata."""

    def __init__(self, config, pattern_name):
        """Set up the message parser."""
        self._message_keys = config['message_keys']
        self._topic = config['topic']
        self.variable_tags = config.get('variable_tags', [])
        self._pattern_name = pattern_name

    @property
    def fmt(self):
        """Get keys of the metadata."""
        return self._message_keys

    def globify(self, mda):
        """Globify the metadata."""
        return (self._pattern_name,) + tuple(mda[key] for key in self._message_keys)

    def parse(self, metadata):
        """Parse the message."""
        return metadata

    def matches(self, msg):
        """Check that the message matches."""
        for key in self._message_keys:
            if key not in msg.data:
                return False
        if msg.subject.startswith(self._topic):
            return True
        return False

    def uid(self, metadata):
        """Get a unique id for the message."""
        return '_'.join(str(metadata[key]) for key in self._message_keys)


class Message:
    """A message object."""

    def __init__(self, posttroll_message, pattern, drop_scheme=False):
        """Set up the message."""
        self.pattern = pattern
        self._drop_scheme = drop_scheme
        posttroll_message = self._handle_scheme(posttroll_message)
        self.message_data = posttroll_message.data
        self.type = posttroll_message.type
        self._posttroll_message = posttroll_message
        self.metadata = pattern.parser.parse(self.message_data)
        self._time_name = self.pattern.time_name
        self.adjust_time_by_flooring()

    @property
    def id_time(self):
        """Return the identifying time of the message."""
        return self.metadata[self._time_name]

    def uid(self):
        """Get a unique id for the message."""
        return self.pattern.parser.uid(self.message_data)

    def adjust_time_by_flooring(self):
        """Floor time to full minutes."""
        self._adjust_time_by_flooring(self.metadata, self.pattern.group_by_minutes, self.pattern.time_name)

    def _adjust_time_by_flooring(self, metadata, group_by_minutes, time_name):
        if group_by_minutes is None:
            return
        time_item = metadata[time_name]
        minutes = time_item.minute
        floor_minutes = int(minutes / group_by_minutes) * group_by_minutes
        time_item = dt.datetime(time_item.year, time_item.month,
                                time_item.day, time_item.hour, floor_minutes, 0)
        metadata[time_name] = time_item

    def _handle_scheme(self, posttroll_message):
        message_data = posttroll_message.data.copy()
        if self._drop_scheme:
            url_parts = urlparse(message_data['uri'])
            uri = urlunparse(
                (
                    '',
                    '',
                    url_parts.path,
                    '',
                    '',
                    ''
                )
            )
            message_data['uri'] = uri
            posttroll_message.data = message_data
        return posttroll_message

    @property
    def filtered_metadata(self):
        """Merge the metadata."""
        meta = filter_metadata(self.metadata, self.message_data,
                               keep_parsed_keys=self.pattern._global_keep_parsed_keys,
                               local_keep_parsed_keys=self.pattern._local_keep_parsed_keys)
        self._adjust_time_by_flooring(meta, self.pattern.group_by_minutes, self.pattern.time_name)
        return meta


class Slot:
    """A time slot class."""

    def __init__(self, timestamp, metadata, patterns, timeliness, num_files_premature_publish):
        """Set up the slot."""
        self.timestamp = str(timestamp)
        self._info = dict()
        self._timeliness = timeliness
        self._num_files_premature_publish = num_files_premature_publish
        self._pattern_keys = patterns.keys()
        self.output_metadata = metadata.copy()
        self['timeout'] = None
        # Critical files that are required, otherwise production will fail.
        # If there are no critical files, empty set([]) is used.
        if len(patterns) == 1:
            self.output_metadata['dataset'] = []
        else:
            self.output_metadata['collection'] = {}
            self.output_metadata.pop('dataset', None)
        for (key, pattern) in patterns.items():
            if len(patterns) > 1:
                self.output_metadata['collection'][key] = \
                    {'dataset': [], 'sensor': []}
            self[key] = self.create_slot_pattern(pattern)

        self.update_timeout()

    def __getitem__(self, item):
        """Get the item."""
        return self._info[item]

    def __setitem__(self, key, value):
        """Set the item."""
        return self._info.__setitem__(key, value)

    def create_slot_pattern(self, pattern):
        """Create slot pattern."""
        slot_pattern = dict()
        is_critical_set = pattern.get("is_critical_set", False)
        slot_pattern['is_critical_set'] = is_critical_set
        slot_pattern['critical_files'] = set([])
        slot_pattern['wanted_files'] = set([])
        slot_pattern['all_files'] = set([])
        slot_pattern['received_files'] = set([])
        slot_pattern['delayed_files'] = dict()
        slot_pattern['missing_files'] = set([])
        slot_pattern['files_till_premature_publish'] = self._num_files_premature_publish
        critical_segments = pattern.get("critical_files", None)
        fname_set = self.compose_filenames(pattern.parser, critical_segments)
        if critical_segments:
            slot_pattern['critical_files'].update(fname_set)

        else:
            if is_critical_set:
                # If critical segments are not defined, but the
                # file based on this pattern is required, add it
                # to critical files
                slot_pattern['critical_files'].update(fname_set)

            # In any case add it to the wanted and all files
            slot_pattern['wanted_files'].update(fname_set)
            slot_pattern['all_files'].update(fname_set)
        # These segments are wanted, but not critical to production
        wanted_segments = pattern.get("wanted_files", None)
        slot_pattern['wanted_files'].update(
            self.compose_filenames(pattern.parser, wanted_segments))
        # Name of all the files
        all_segments = pattern.get("all_files", None)
        slot_pattern['all_files'].update(
            self.compose_filenames(pattern.parser, all_segments))
        return slot_pattern

    def compose_filenames(self, parser, itm_str):
        """Compose filename set()s based on a pattern and item string.

        itm_str is formated like ':PRO,:EPI' or 'VIS006:8,VIS008:1-8,...'
        """
        # Handle missing itm_str
        if itm_str in (None, ''):
            itm_str = ':'

        # Get copy of metadata
        meta = self.output_metadata.copy()

        # Replace variable tags (such as processing time) with
        # wildcards, as these can't be forecasted.
        var_tags = parser.variable_tags
        meta = _copy_without_ignore_items(meta,
                                          ignored_keys=var_tags)

        result = set()
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

            segments = _create_segment_list(segments)

            meta['channel_name'] = channel_name
            for seg in segments:
                meta['segment'] = seg
                fname = parser.globify(meta)

                result.add(fname)

        return result

    def update_timeout(self):
        """Update the timeout."""
        timeout = dt.datetime.utcnow() + self._timeliness
        self['timeout'] = timeout
        logger.info("Setting timeout to %s for slot %s.",
                    str(timeout), self.timestamp)

    def add_file(self, message):
        """Add file to the correct filelist."""
        mask, should_be_added = self.is_relevant(message)

        if not should_be_added:
            return

        self._add_message_to_metadata(message)
        uid = message.uid()

        pattern = message.pattern

        slot_pattern = self[pattern.name]
        # If critical files have been received but the slot is
        # not complete, add the file to list of delayed files
        timeout = self['timeout']
        if len(slot_pattern['critical_files']) > 0 and \
           slot_pattern['critical_files'].issubset(slot_pattern['received_files']):
            delay = dt.datetime.utcnow() - (timeout - self._timeliness)
            if delay.total_seconds() > 0:
                slot_pattern['delayed_files'][uid] = delay.total_seconds()

        # Add to received files
        slot_pattern['received_files'].add(mask)
        logger.info("%s processed", uid)

    def _add_message_to_metadata(self, message):
        pattern = message.pattern
        msg_data = message.message_data
        # Add uid and uri
        slot_metadata = self.output_metadata
        if 'collection' in slot_metadata:
            metadata = slot_metadata['collection'][pattern.name]
        else:
            metadata = slot_metadata

        # add uri, uid and sensor

        self._add_file_info_to_metadata(metadata, message)
        self._update_metadata_times(metadata, message)

        # Collect all sensors, not only the latest
        sensors = metadata.get('sensor', [])
        if not isinstance(msg_data["sensor"], (tuple, list, set)):
            msg_data["sensor"] = [msg_data["sensor"]]
        if not isinstance(sensors, list):
            sensors = [sensors]
        for sensor in msg_data["sensor"]:
            if sensor not in sensors:
                sensors.append(sensor)
        slot_metadata['sensor'] = sensors

    def _add_file_info_to_metadata(self, metadata, message):
        msg_data = message.message_data
        if message.type == 'file':
            metadata['dataset'].append({'uri': msg_data['uri'], 'uid': msg_data['uid']})
        elif message.type == 'dataset':
            metadata['dataset'].extend(message.message_data['dataset'])
        else:
            raise NotImplementedError('Cannot handle message of type: ' + str(message.type))

    def _update_metadata_times(self, metadata, message):
        """Update start/end time when message added to metadata."""
        if "start_time" in metadata and "start_time" in message.message_data:
            metadata["start_time"] = min(metadata["start_time"], message.message_data["start_time"])
        if "end_time" in metadata and "end_time" in message.message_data:
            metadata["end_time"] = max(metadata["end_time"], message.message_data["end_time"])

    def is_relevant(self, message):
        """Check if the message is relevant to this slot."""
        slot_pattern = self[message.pattern.name]
        should_be_added = True
        # Replace variable tags (such as processing time) with
        # wildcards, as these can't be forecasted.
        ignored_keys = message.pattern.get('variable_tags', [])
        metadata = _copy_without_ignore_items(message.metadata, ignored_keys=ignored_keys)
        mask = message.pattern.parser.globify(metadata)
        if mask in slot_pattern['received_files']:
            logger.debug("File already received")
            should_be_added = False
        if mask not in slot_pattern['all_files']:
            logger.debug("%s not in %s", mask, slot_pattern['all_files'])
            should_be_added = False
        return mask, should_be_added

    def get_status(self):
        """Determine if slot is complete."""
        status = {}
        num_files = {}
        for key in self._pattern_keys:
            # Default
            status[key] = Status.SLOT_NOT_READY
            if not self[key]['is_critical_set']:
                status[key] = Status.SLOT_NONCRITICAL_NOT_READY

            wanted_and_critical_files = self[key][
                'wanted_files'].union(self[key]['critical_files'])
            num_wanted_and_critical = len(
                wanted_and_critical_files & self[key]['received_files'])

            num_files[key] = num_wanted_and_critical

            if num_wanted_and_critical == self[key]['files_till_premature_publish']:
                self[key]['files_till_premature_publish'] = -1
                status[key] = Status.SLOT_READY_BUT_WAIT_FOR_MORE

            if wanted_and_critical_files.issubset(self[key]['received_files']):
                status[key] = Status.SLOT_READY

        # Determine overall status
        return self.get_collection_status(status, self['timeout'])

    def get_collection_status(self, status, timeout):
        """Determine the overall status of the collection."""
        if len(status) == 0:
            return Status.SLOT_NOT_READY

        status_values = list(status.values())
        if all([val == Status.SLOT_READY for val in status_values]):
            logger.info("Required files received "
                        "for slot %s.", self.timestamp)
            return Status.SLOT_READY

        if dt.datetime.utcnow() > timeout:
            if (Status.SLOT_NONCRITICAL_NOT_READY in status_values and
                (Status.SLOT_READY in status_values or
                    Status.SLOT_READY_BUT_WAIT_FOR_MORE in status_values)):
                return Status.SLOT_READY
            if (Status.SLOT_READY_BUT_WAIT_FOR_MORE in status_values and
                    Status.SLOT_NOT_READY not in status_values):
                return Status.SLOT_READY
            if all([val == Status.SLOT_NONCRITICAL_NOT_READY for val in
                    status_values]):
                for key in status.keys():
                    if len(self[key]['received_files']) > 0:
                        return Status.SLOT_READY
                return Status.SLOT_OBSOLETE_TIMEOUT

            logger.warning("Timeout occured and required files "
                           "were not present, data discarded for "
                           "slot %s.",
                           self.timestamp)
            return Status.SLOT_OBSOLETE_TIMEOUT

        if Status.SLOT_NOT_READY in status_values:
            return Status.SLOT_NOT_READY
        if Status.SLOT_NONCRITICAL_NOT_READY in status_values:
            return Status.SLOT_NONCRITICAL_NOT_READY
        if Status.SLOT_READY_BUT_WAIT_FOR_MORE in status_values:
            return Status.SLOT_READY_BUT_WAIT_FOR_MORE


def _create_segment_list(segments):
    segments = segments.split('-')
    if len(segments) == 2:
        try:
            range_start = int(segments[0])
            range_end = int(segments[-1]) + 1
        except ValueError:
            segments = ['-'.join(segments)]
        else:
            format_string = '%d'
            if len(segments[0]) > 1 and segments[0][0] == '0':
                format_string = '%0' + str(len(segments[0])) + 'd'
            segments = [format_string % i
                        for i in range(range_start,
                                       range_end)]
    return segments


class Pattern:
    """A pattern to watch for."""

    def __init__(self, name, pattern_config, defaults):
        """Set up the pattern."""
        self._config = pattern_config
        try:
            self.parser = UIDParser(pattern_config, name)
        except KeyError:
            self.parser = MessageParser(pattern_config, name)
        self.name = name
        self.timeliness = defaults.get('timeliness', 1200)
        if "start_time_pattern" in pattern_config:
            logger.debug("Create start time pattern %s", name)
            self._create_start_time_pattern()

        self._global_keep_parsed_keys = defaults.get('keep_parsed_keys', [])
        self._local_keep_parsed_keys = pattern_config.get('keep_parsed_keys', [])
        self._group_by_minutes = self._config.get('group_by_minutes', defaults.get('group_by_minutes'))
        self.time_name = self._config.get('time_name', defaults.get('time_name', 'start_time'))

    @property
    def group_by_minutes(self):
        """Group by minutes."""
        return self._group_by_minutes

    def __getitem__(self, item):
        """Get the item."""
        return self._config[item]

    def __setitem__(self, key, value):
        """Set the item."""
        return self._config.__setitem__(key, value)

    def get(self, *args, **kwargs):
        """Get the item."""
        return self._config.get(*args, **kwargs)

    def __contains__(self, item):
        """Check if contains item."""
        return self._config.__contains__(item)

    def _create_start_time_pattern(self):
        """Convert check time into int minutes variables."""
        time_conf = self["start_time_pattern"]
        start_time_str = time_conf.get("start_time", "00:00")
        end_time_str = time_conf.get("end_time", "23:59")
        delta_time_str = time_conf.get("delta_time", "00:01")

        start_h, start_m = start_time_str.split(':')
        end_h, end_m = end_time_str.split(':')
        delta_h, delta_m = delta_time_str.split(':')
        interval = {"start": (60 * int(start_h)) + int(start_m),
                    "end": (60 * int(end_h)) + int(end_m),
                    "delta": (60 * int(delta_h)) + int(delta_m),
                    "midnight": False}

        # Start-End time across midnight
        if interval["start"] > interval["end"]:
            interval["end"] += 24 * 60
            interval["midnight"] = True
        self["_start_time_pattern"] = interval
        logger.debug("Filter start:%s end:%s delta:%s",
                     start_time_str, end_time_str,
                     delta_time_str)


class SegmentGatherer(object):
    """Gatherer for geostationary satellite segments and multifile polar satellite granules."""

    _listener = None
    _publisher = None

    def __init__(self, config):
        """Initialize the segment gatherer."""
        self._config = config.copy()
        self._pattern_configs = self._config.pop('patterns')
        self._subject = None
        self._timeliness = dt.timedelta(seconds=config.get("timeliness", 1200))

        # This get the 'keep_parsed_keys' valid for all patterns
        self._keep_parsed_keys = self._config.get('keep_parsed_keys', [])

        self._patterns = self._create_patterns()

        self._elements = list(self._patterns.keys())

        self._time_tolerance = self._config.get("time_tolerance", 30)
        self._bundle_datasets = self._config.get("bundle_datasets", False)

        self._num_files_premature_publish = self._config.get("num_files_premature_publish", -1)

        self.slots = OrderedDict()

        self.time_name = self._config.get('time_name', 'start_time')
        # Floor the scene start time to the given full minutes
        self._group_by_minutes = self._config.get('group_by_minutes', None)

        self._loop = False
        self._providing_server = self._config.get('providing_server')
        self._is_first_message_after_start = True

    def _create_patterns(self):
        return {key: Pattern(key, pattern_config, self._config)
                for key, pattern_config in self._pattern_configs.items()}

    def _clear_slot(self, time_slot):
        """Clear data."""
        if time_slot in self.slots:
            del self.slots[time_slot]

    def _reinitialize_gatherer(self, time_slot, missing_files_check=True):
        """Publish file dataset and reinitialize gatherer."""
        slot = self.slots[time_slot]

        # Diagnostic logging about delayed ...
        delayed_files = {}
        for key in self._elements:
            delayed_files.update(slot[key]['delayed_files'])
        if len(delayed_files) > 0:
            file_str = ''
            for key, value in delayed_files.items():
                file_str += "%s %f seconds, " % (key, value)
            logger.warning("Files received late: %s", file_str.strip(', '))

        # ... and missing files
        if missing_files_check:
            missing_files = set([])
            for key in self._elements:
                missing_files = slot[key]['all_files'].difference(
                    slot[key]['received_files'])
            if len(missing_files) > 0:
                logger.warning("Missing files: %s", ', '.join((str(missing) for missing in missing_files)))

        # Remove tags that are not necessary for datasets
        for tag in REMOVE_TAGS:
            try:
                del slot.output_metadata[tag]
            except KeyError:
                pass

        output_metadata = slot.output_metadata.copy()

        if self._bundle_datasets and "dataset" not in output_metadata:
            output_metadata["dataset"] = []
            for collection in output_metadata["collection"].values():
                output_metadata["dataset"].extend(collection['dataset'])
            del output_metadata["collection"]

        self._publish(output_metadata)

    def _publish(self, metadata):
        if "dataset" in metadata:
            msg = pmessage.Message(self._subject, "dataset", metadata)
        else:
            msg = pmessage.Message(self._subject, "collection", metadata)
        logger.info("Sending: %s", str(msg))
        self._publisher.send(str(msg))

    def _generate_publish_service_name(self):
        publish_service_name = "segment_gatherer"
        for key in sorted(self._elements):
            publish_service_name += "_" + str(key)
        return publish_service_name

    def _setup_messaging(self):
        """Set up messaging."""
        self._setup_listener()
        self._setup_publisher()

    def _setup_listener(self):
        self._subject = self._config['posttroll']['publish_topic']
        topics = self._config['posttroll'].get('topics')
        addresses = self._config['posttroll'].get('addresses')
        services = self._config['posttroll'].get('services', "")
        nameserver = check_nameserver_options(self._config['posttroll'].get('nameservers'),
                                              for_listener=True)

        self._listener = ListenerContainer(
            topics=topics,
            addresses=addresses,
            nameserver=nameserver,
            services=services
        )

    def _setup_publisher(self):
        self._publisher = create_started_publisher_from_config(self._collect_publisher_config())

    def _collect_publisher_config(self):
        publish_port = self._config['posttroll'].get('publish_port', 0)
        nameservers = check_nameserver_options(self._config['posttroll'].get('nameservers', []))

        # Name each segment_gatherer with the section/patterns name.
        # This way the user can subscribe to a specific segment_gatherer service instead of all.
        publish_service_name = self._generate_publish_service_name()
        return create_publisher_config_dict(publish_service_name, nameservers, publish_port)

    def run(self):
        """Run SegmentGatherer."""
        self._setup_messaging()

        self._loop = True
        while self._loop:
            self.triage_slots()

            # Check listener for new messages
            try:
                msg = self._listener.output_queue.get(True, 1)
            except AttributeError:
                msg = self._listener.queue.get(True, 1)
            except KeyboardInterrupt:
                self.stop()
                continue
            except Empty:
                continue

            if msg.type in ["file", "dataset"]:
                # If providing server is configured skip message if not from providing server
                if self._providing_server and self._providing_server != msg.host:
                    continue
                logger.info("New message received: %s", str(msg))
                self.process(msg)

    def triage_slots(self):
        """Check if there are slots ready for publication."""
        slots = self.slots.copy()
        for slot_time, slot in slots.items():
            slot_time = str(slot_time)
            status = slot.get_status()
            if status == Status.SLOT_READY:
                # Collection ready, publish and remove
                self._reinitialize_gatherer(slot_time)
                self._clear_slot(slot_time)
            if status == Status.SLOT_READY_BUT_WAIT_FOR_MORE:
                # Collection ready, publish and but wait for more
                self._reinitialize_gatherer(slot_time, missing_files_check=False)
            elif status == Status.SLOT_OBSOLETE_TIMEOUT:
                # Collection unfinished and obsolete, discard
                self._clear_slot(slot_time)
            else:
                # Collection unfinished, wait for more data
                pass

    def stop(self):
        """Stop gatherer."""
        logger.info("Stopping gatherer.")
        self._loop = False
        if self._listener is not None:
            if self._listener.thread is not None:
                self._listener.stop()
        if self._publisher is not None:
            self._publisher.stop()

    def process(self, msg):
        """Process message."""
        # Find the correct parser for this file
        try:
            message = self.message_from_posttroll(msg)
            pattern = message.pattern
        except TypeError:
            logger.debug("No parser matching message, skipping.")
            return

        # Check if time of the raw is in scheduled range
        if "_start_time_pattern" in pattern:
            schedule_ok = self.check_if_time_is_in_interval(
                pattern["_start_time_pattern"],
                message.id_time)
            if not schedule_ok:
                logger.debug("Hour pattern '%s' skip: %s" +
                             " for start_time: %s",
                             pattern.name, message.uid(),
                             message.id_time.strftime("%H:%M"))
                return

        slot_time = self._find_time_slot(message.id_time)

        # Init metadata etc if this is the first file
        if slot_time not in self.slots:
            slot = self._create_slot(message)
        else:
            slot = self.slots[slot_time]

        slot.add_file(message)
        self.check_and_add_existing_files(slot, message)

    def message_from_posttroll(self, msg):
        """Create a message object from a posttroll message instance."""
        for pattern in self._patterns.values():
            try:
                if pattern.parser.matches(msg):
                    drop_scheme = self._config.get('all_files_are_local', False)
                    return Message(msg, pattern, drop_scheme=drop_scheme)
            except KeyError as err:
                logger.debug("No key %s in message.", str(err))
        raise TypeError

    def _find_time_slot(self, time_obj):
        """Find time slot and return the slot as a string.

        If no slots are close enough, return *str(time_obj)*
        """
        for slot in self.slots:
            time_slot = self.slots[slot].output_metadata[self.time_name]
            time_diff = time_obj - time_slot
            if abs(time_diff.total_seconds()) < self._time_tolerance:
                logger.debug("Found existing time slot at %s, using that",
                             str(time_slot))
                return slot

        return str(time_obj)

    def _create_slot(self, message):
        """Init wanted, all and critical files."""
        timestamp = message.id_time
        logger.debug(f"Adding new slot: {timestamp}")

        slot = Slot(timestamp, message.filtered_metadata, self._patterns, self._timeliness,
                    self._num_files_premature_publish)
        self.slots[str(timestamp)] = slot
        return slot

    def check_if_time_is_in_interval(self, time_range, raw_start_time):
        """Check if raw time is inside configured interval."""
        time_ok = False

        # Convert check time into int variables
        raw_time = (60 * raw_start_time.hour) + raw_start_time.minute
        if time_range["midnight"] and raw_time < time_range["start"]:
            raw_time += 24 * 60

        # Check start and end time
        if time_range["start"] <= raw_time <= time_range["end"]:
            # Raw time in range, check interval
            if ((raw_time - time_range["start"]) % time_range["delta"]) == 0:
                time_ok = True

        return time_ok

    def check_and_add_existing_files(self, slot, message):
        """Check for existing files in the uri basedir and add them to the slot."""
        if self._should_check_for_existing_files(message):
            # Disable debug logging temporarily
            logging.disable(logging.DEBUG)
            fnames = _get_existing_files_from_message(message)
            logger.debug("Checking %d pre-existing files after restart.", len(fnames))
            self._add_existing_files_to_slot(slot, fnames, message)
            # Restore the original logging level
            logging.disable(logging.NOTSET)

    def _should_check_for_existing_files(self, message):
        if not self._config.get("check_existing_files_after_start", False):
            return False
        if not self._is_first_message_after_start:
            return False
        if message.type != "file":
            logger.error("Only 'file' messages are supported.")
            return False
        self._is_first_message_after_start = False
        return True

    def _add_existing_files_to_slot(self, slot, fnames, message):
        for fname in fnames:
            meta = {
                "uid": os.path.basename(fname),
                "uri": fname,
                "sensor": message._posttroll_message.data["sensor"]
                }
            msg = self.message_from_posttroll(pmessage.Message(message._posttroll_message.subject, "file", meta))
            slot.add_file(msg)


def _get_existing_files_from_message(message):
    mask = message.pattern.parser.globify({})
    url_parts = urlparse(message.message_data["uri"])

    return _fsspec_glob(url_parts, mask)


def _fsspec_glob(url_parts, mask):
    import fsspec

    pattern = urlunparse(
        (
            url_parts.scheme,
            url_parts.netloc,
            '/'.join(url_parts.path.split('/')[:-1]) + '/' + mask,
            '',
            '',
            ''
        )
    )

    fs_ = fsspec.filesystem(url_parts.scheme)
    files = fs_.glob(pattern)
    # There might be no scheme in the returned filenames, so add it if scheme is defined
    if url_parts.scheme:
        files = [url_parts.scheme + '://' + f for f in files if not f.startswith(
            url_parts.scheme + '://')]
    return files


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
    from configparser import RawConfigParser, NoOptionError

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

    try:
        conf['time_name'] = config.get(section, "time_name")
    except (NoOptionError, ValueError):
        conf['time_name'] = 'start_time'

    try:
        conf['check_existing_files_after_start'] = config.getboolean(section, "check_existing_files_after_start")
    except (NoOptionError, ValueError):
        conf['check_existing_files_after_start'] = False

    try:
        conf['all_files_are_local'] = config.getboolean(section, "all_files_are_local")
    except (NoOptionError, ValueError):
        conf['all_files_are_local'] = False

    return conf


def filter_metadata(mda, msg_data, keep_parsed_keys=None, local_keep_parsed_keys=None):
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
    for key in msg_data:
        # If time name is given, do not overwrite it
        if key not in DO_NOT_COPY_KEYS and key not in keep_parsed_keys and key not in local_keep_parsed_keys:
            metadata[key] = msg_data[key]

    return metadata
