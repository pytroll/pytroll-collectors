#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012 - 2021, 2026 Pytroll developers
#
# Author(s):
#
#   Kristian Rune Larsen <krl@dmi.dk>
#   Martin Raspaud <martin.raspaud@smhi.se>
#   Panu Lahtinen <panu.lahtinen@fmi.fi>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Base classes and helper functions for region_collectors."""

import datetime as dt
import logging
from threading import Thread, Event
import os

from trollsift import compose, Parser
from posttroll import message

from pytroll_collectors.utils import fix_start_end_time
from pytroll_collectors.utils import ensure_utc_aware

logger = logging.getLogger(__name__)



def total_seconds(tdef):
    """Calculate total time in seconds."""
    return ((tdef.microseconds +
             (tdef.seconds + tdef.days * 24 * 3600) * 10 ** 6) / 10.0 ** 6)


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
                now = dt.datetime.now(dt.timezone.utc)
                next_utc_aware_timeout = ensure_utc_aware(next_timeout[1])
                if next_utc_aware_timeout and (next_utc_aware_timeout < now):
                    logger.debug("Timeout detected, terminating collector")
                    logger.debug("Area: %s, timeout: %s",
                                 next_timeout[0].region,
                                 str(next_utc_aware_timeout))
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
                                 str(total_seconds(next_utc_aware_timeout -
                                                   dt.datetime.now(dt.timezone.utc))))
                    logger.debug("Is last file added: {}".format(next_timeout[0].is_last_file_added()))
                    if self.publish_message_after_each_reception and next_timeout[0].is_last_file_added():
                        # If this option is given:
                        # Publish message after each new file is reveived
                        # and added to the collection
                        # but don't clean up the collection as new files will be added until timeout
                        self.publish_collection(next_timeout[0].finish_without_reset())
                    self.new_file.wait(total_seconds(next_utc_aware_timeout - now))
                    self.new_file.clear()
            else:
                self.new_file.wait()
                self.new_file.clear()

    def stop(self):
        """Stop everything."""
        self._running = False
        self.new_file.set()
