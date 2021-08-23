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

from posttroll.subscriber import NSSubscriber

from ._base import FileTrigger, fix_start_end_time

logger = logging.getLogger(__name__)


class AbstractMessageProcessor(Thread):
    """Process Messages."""

    def __init__(self, services, topics, nameserver="localhost"):
        """Init the message processor."""
        super(AbstractMessageProcessor, self).__init__()
        logger.debug("Nameserver: {}".format(nameserver))
        self.nssub = NSSubscriber(services, topics, True, nameserver=nameserver)
        self.sub = None
        self.loop = True

    def start(self):
        """Start the processor."""
        self.sub = self.nssub.start()
        Thread.start(self)

    def process(self, msg):
        """Process the message."""
        del msg
        raise NotImplementedError("process is not implemented!")

    def run(self):
        """Run the trigger."""
        try:
            for msg in self.sub.recv(2):
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
        self.nssub.stop()
        self.loop = False


class PostTrollTrigger(FileTrigger):
    """Get posttroll messages."""

    def __init__(self, collectors, services, topics, publisher, duration=None,
                 publish_topic=None, nameserver="localhost",
                 publish_message_after_each_reception=False):
        """Init the posttroll trigger."""
        self.duration = duration
        self.msgproc = AbstractMessageProcessor(services, topics, nameserver=nameserver)
        self.msgproc.process = self.add_file
        FileTrigger.__init__(self, collectors, None, publisher, publish_topic=publish_topic,
                             publish_message_after_each_reception=publish_message_after_each_reception)

    def start(self):
        """Start the posttroll trigger."""
        FileTrigger.start(self)
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
        FileTrigger.stop(self)
