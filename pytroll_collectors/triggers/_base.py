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

"""Base classes and helper functions for region_collectors."""

from datetime import datetime, timedelta
import logging
from threading import Thread, Event
import os

from trollsift import compose, Parser
from posttroll import message

logger = logging.getLogger(__name__)


def total_seconds(tdef):
    """Calculate total time in seconds."""
    return ((tdef.microseconds +
             (tdef.seconds + tdef.days * 24 * 3600) * 10 ** 6) / 10.0 ** 6)


def fix_start_end_time(mda):
    """Make start and end time coherent."""
    if "duration" in mda and "end_time" not in mda:
        mda["end_time"] = (mda["start_time"]
                           + timedelta(seconds=int(mda["duration"])))
    if "start_date" in mda:
        mda["start_time"] = datetime.combine(mda["start_date"].date(),
                                             mda["start_time"].time())
        if "end_date" not in mda:
            mda["end_date"] = mda["start_date"]
        del mda["start_date"]
    if "end_date" in mda:
        mda["end_time"] = datetime.combine(mda["end_date"].date(),
                                           mda["end_time"].time())
        del mda["end_date"]

    while mda["start_time"] > mda["end_time"]:
        mda["end_time"] += timedelta(days=1)

    if "duration" in mda:
        del mda["duration"]

    return mda


class Trigger:
    """Abstract trigger class."""

    def __init__(self, collectors, publisher, publish_topic=None):
        """Init the trigger."""
        self.collectors = collectors
        self.publisher = publisher
        self.publish_topic = publish_topic

    def _process_metadata(self, metadata):
        """Execute the collectors and publish the collection."""
        if not metadata:
            logger.warning("No metadata")
            return
        for collector in self.collectors:
            try:
                res = collector(metadata.copy())
            except KeyError as ke:
                logger.exception("collector failed with: %s ", str(ke))
            else:
                if res:
                    self.publish_collection(res)

    def publish_collection(self, metadata):
        """Terminate the gathering."""
        subject = self._get_topic(metadata[0])
        mda = _merge_metadata(metadata)

        if mda:
            msg = message.Message(subject, "collection", mda)
            logger.info("sending %s", str(msg))
            self.publisher.send(str(msg))
        else:
            logger.warning("Malformed metadata, no key: %s", "uri")

    def _get_topic(self, mda):
        if self.publish_topic is not None:
            logger.debug("Composing topic.")
            subject = compose(self.publish_topic, mda)
        else:
            logger.debug("Using default topic.")
            subject = "/".join(("", mda["format"], mda["data_processing_level"], ''))
        return subject


def _merge_metadata(metadata):
    mda = metadata[0].copy()
    sorted_mda = sorted(metadata, key=lambda x: x["start_time"])
    mda['start_time'] = sorted_mda[0]['start_time']
    mda['end_time'] = sorted_mda[-1]['end_time']
    mda['collection_area_id'] = sorted_mda[-1]['collection_area_id']
    mda['collection'] = []

    is_correct = False
    for meta in sorted_mda:
        new_mda = {}
        if "uri" in meta or 'dataset' in meta:
            is_correct = True
        for key in ['dataset', 'uri', 'uid']:
            if key in meta:
                new_mda[key] = meta[key]
            new_mda['start_time'] = meta['start_time']
            new_mda['end_time'] = meta['end_time']
        mda['collection'].append(new_mda)

    for key in ['dataset', 'uri', 'uid']:
        if key in mda:
            del mda[key]

    if is_correct:
        return mda
    return None


class FileTrigger(Trigger, Thread):
    """File trigger, acting upon inotify events."""

    def __init__(self, collectors, config_items, publisher,
                 publish_topic=None, publish_message_after_each_reception=False):
        """Init the file trigger."""
        Thread.__init__(self)
        Trigger.__init__(self, collectors, publisher, publish_topic=publish_topic)
        self._config_items = config_items
        self._running = True
        self.new_file = Event()
        self.publish_message_after_each_reception = publish_message_after_each_reception

    def _get_metadata(self, fname):
        """Parse metadata from the file."""
        parser = Parser(self._config_items["pattern"])

        res = parser.parse(fname)
        res.update(dict(self._config_items))

        for key in ["watcher", "pattern", "timeliness", "regions"]:
            res.pop(key, None)

        res = fix_start_end_time(res)

        if ("sensor" in res) and ("," in res["sensor"]):
            res["sensor"] = res["sensor"].split(",")

        res["uri"] = fname
        res["filename"] = os.path.basename(fname)

        return res

    def _process_pathname(self, pathname):
        mda = self._get_metadata(pathname)
        logger.debug("mda: %s", str(mda))
        Trigger._process_metadata(self, mda)

    def add_file(self, pathname):
        """React to arrival of a file."""
        self._process_pathname(pathname)
        self.new_file.set()

    def run(self):
        """Handle the timeouts."""
        # The wait for new files is handled through the event mechanism of the
        # threading module:
        # - first a new file arrives, and an event is triggered
        # - then the new timeouts are computed
        # - if a timeout occurs during the wait, the wait is interrupted and
        #   the timeout is handled.
        while self._running:
            timeouts = [(collector, collector.timeout)
                        for collector in self.collectors
                        if collector.timeout is not None]

            if timeouts:
                next_timeout = min(timeouts, key=(lambda x: x[1]))
                if next_timeout[1] and (next_timeout[1] < datetime.utcnow()):
                    logger.debug("Timeout detected, terminating collector")
                    logger.debug("Area: %s, timeout: %s",
                                 next_timeout[0].region,
                                 str(next_timeout[1]))
                    if self.publish_message_after_each_reception:
                        # If this options is given:
                        # Dont send message as it is assumed this was send
                        # when the last message was received.
                        # Only clean up the collector.
                        next_timeout[0].finish()
                    else:
                        self.publish_collection(next_timeout[0].finish())
                else:
                    logger.debug("Waiting %s seconds until timeout",
                                 str(total_seconds(next_timeout[1] -
                                                   datetime.utcnow())))
                    logger.debug("Is last file added: {}".format(next_timeout[0].is_last_file_added()))
                    if self.publish_message_after_each_reception and next_timeout[0].is_last_file_added():
                        # If this option is given:
                        # Publish message after each new file is reveived
                        # and added to the collection
                        # but don't clean up the collection as new files will be added until timeout
                        self.publish_collection(next_timeout[0].finish_without_reset())
                    self.new_file.wait(total_seconds(next_timeout[1] -
                                                     datetime.utcnow()))
                    self.new_file.clear()
            else:
                self.new_file.wait()
                self.new_file.clear()

    def stop(self):
        """Stop everything."""
        self._running = False
        self.new_file.set()
