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

"""Inotify trigger for region_collectors."""

import logging
import os
from threading import Event
from fnmatch import fnmatch

from pyinotify import (IN_CLOSE_WRITE, IN_MOVED_TO, Notifier, ProcessEvent,
                       WatchManager)

from ._base import FileTrigger

logger = logging.getLogger(__name__)


class InotifyTrigger(ProcessEvent, FileTrigger):
    """File trigger, acting upon inotify events."""

    def __init__(self, collectors, publisher, config, patterns,
                 publish_topic=None):
        """Init the inotify trigger."""
        ProcessEvent.__init__(self)
        FileTrigger.__init__(self, collectors, config, publisher, publish_topic=publish_topic)
        self.input_dirs = []
        for pattern in patterns:
            self.input_dirs.append(os.path.dirname(pattern))
        self.patterns = patterns
        self.new_file = Event()

    def process_IN_CLOSE_WRITE(self, event):
        """Process a closing file."""
        for pattern in self.patterns:
            if fnmatch(event.src_path, pattern):
                logger.debug("New file detected (close write): %s",
                             event.pathname)
                self.add_file(event.pathname)

    def process_IN_MOVED_TO(self, event):
        """Process a file moving into the directory."""
        for pattern in self.patterns:
            if fnmatch(event.src_path, pattern):
                logger.debug("New file detected (moved to): %s",
                             event.pathname)
                self.add_file(event.pathname)

    def loop(self):
        """Loop until done."""
        self.start()
        try:
            # inotify interface
            wm_ = WatchManager()
            mask = IN_CLOSE_WRITE | IN_MOVED_TO

            # create notifier
            notifier = Notifier(wm_, self)

            # add watches
            for idir in self.input_dirs:
                wm_.add_watch(idir, mask)

            # loop forever
            notifier.loop()
        finally:
            self.stop()
            self.join()
