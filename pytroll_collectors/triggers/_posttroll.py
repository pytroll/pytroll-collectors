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

"""Posttroll trigger for region_collectors."""

from threading import Thread
import logging
import warnings

from posttroll.subscriber import create_subscriber_from_dict_config

from ._base import FileTrigger, fix_start_end_time

logger = logging.getLogger(__name__)


class _MessageProcessor(Thread):
    """Process Messages."""

    def __init__(self, services, topics, nameserver=None, inbound_connection=None):
        """Init the message processor."""
        super().__init__()
        if nameserver:
            warnings.warn(PendingDeprecationWarning(
                "`nameserver` for subscription should be replaced with `inbound_connection"
            ))
        logger.debug("Nameserver: {}".format(nameserver))
        config_for_subscriber = create_subscriber_config(services, topics, nameserver, inbound_connection)
        self.subscriber = create_subscriber_from_dict_config(config_for_subscriber)
        self.loop = True

    def start(self):
        """Start the processor."""
        Thread.start(self)

    def process(self, msg):
        """Process the message."""
        del msg
        raise NotImplementedError("process is not implemented!")

    def run(self):
        """Run the trigger."""
        try:
            for msg in self.subscriber.recv(2):
                if not self.loop:
                    break
                if msg is None:
                    continue
                if msg.type not in ('file', 'collection', 'dataset'):
                    continue
                self.process(msg)
        finally:
            self.stop()

    def stop(self):
        """Stop the trigger."""
        self.subscriber.stop()
        self.loop = False


def create_subscriber_config(services, topics, nameserver, inbound_connection):
    """Create the subscriber config dictionary."""
    config_for_subscriber = dict(services=services, topics=topics, nameserver=nameserver, addr_listener=True)
    if inbound_connection:
        addresses, nameservers = _split_inbound_connection(inbound_connection)
        if len(nameservers) == 1:
            nameserver = nameservers[0]
        config_for_subscriber["addresses"] = addresses
    if nameserver is None:
        if inbound_connection:
            nameserver = False
        else:
            nameserver = "localhost"
    config_for_subscriber["nameserver"] = nameserver
    return config_for_subscriber


def _split_inbound_connection(inbound_connection):
    addresses = []
    nameservers = []
    for address in inbound_connection:
        if ":" in address:
            addresses.append("tcp://" + address)
        else:
            nameservers.append(address)
    if len(nameservers) > 1:
        raise ValueError("Only one nameserver (address without a port) can be provided.")
    return addresses, nameservers


class PostTrollTrigger(FileTrigger):
    """Get posttroll messages."""

    def __init__(self, collectors, services, topics, publisher, duration=None,
                 publish_topic=None, nameserver=None,
                 inbound_connection=None,
                 publish_message_after_each_reception=False):
        """Init the posttroll trigger."""
        self.duration = duration
        self.msgproc = _MessageProcessor(services, topics, nameserver=nameserver, inbound_connection=inbound_connection)
        self.msgproc.process = self.add_file
        super().__init__(collectors, None, publisher, publish_topic=publish_topic,
                         publish_message_after_each_reception=publish_message_after_each_reception)

    def start(self):
        """Start the posttroll trigger."""
        super().start()
        self.msgproc.start()

    def _get_metadata(self, message):
        """Return the message data."""
        # Include file duration in message data
        if self.duration:
            message.data["duration"] = self.duration

        # Fix start and end time
        try:
            mgs_data = fix_start_end_time(message.data)
        except KeyError:
            logger.exception("Something went wrong!")
        else:
            return mgs_data

    def stop(self):
        """Stop the posttroll trigger."""
        self.msgproc.stop()
        super().stop()
