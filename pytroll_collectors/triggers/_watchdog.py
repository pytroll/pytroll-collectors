#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2014 - 2021 Pytroll developers
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

"""Watchdog trigger for region_collectors."""

from threading import Event
from fnmatch import fnmatch
import logging
import os

from watchdog.events import FileSystemEventHandler
from watchdog.observers.polling import PollingObserver
from watchdog.observers import Observer

from ._base import FileTrigger

logger = logging.getLogger(__name__)


class AbstractWatchDogProcessor(FileSystemEventHandler):
    """File trigger, acting upon file system events."""

    cases = {"PollingObserver": PollingObserver,
             "Observer": Observer}

    def __init__(self, patterns, observer_class_name="Observer"):
        """Init the processor."""
        FileSystemEventHandler.__init__(self)
        self.input_dirs = []
        for pattern in patterns:
            self.input_dirs.append(os.path.dirname(pattern))
            logger.debug("watching %s", str(os.path.dirname(pattern)))
        self.patterns = patterns

        self.new_file = Event()
        self.observer = self.cases.get(observer_class_name, Observer)()

    def on_created(self, event):
        """Process creating a file."""
        self._process(event.src_path)

    def on_moved(self, event):
        """Process a file being moved to the destination directory."""
        self._process(event.dest_path)

    def _process(self, pathname):
        """Process a file."""
        try:
            for pattern in self.patterns:
                if fnmatch(pathname, pattern):
                    logger.debug("New file detected: %s", pathname)
                    self.process(pathname)
                    logger.debug("Done processing file")
                    return
        except Exception:
            logger.exception(
                "Something wrong happened in the event processing!")

    def process(self, pathname):
        """Process, abstract."""
        raise NotImplementedError

    def start(self):
        """Start processor."""
        # add watches
        for idir in self.input_dirs:
            self.observer.schedule(self, idir)
        self.observer.start()

        logger.debug("Started watching filesystem")

    def stop(self):
        """Stop processor."""
        self.observer.stop()
        self.observer.join()


class WatchDogTrigger(FileTrigger):
    """File trigger, acting upon filesystem events."""

    def __init__(self, collectors, config_items, patterns, observer_class_name, publisher,
                 publish_topic=None):
        """Init the trigger."""
        self.wdp = AbstractWatchDogProcessor(patterns, observer_class_name)
        super().__init__(collectors, config_items, publisher,
                         publish_topic=publish_topic)
        self.wdp.process = self.add_file

    def start(self):
        """Start the trigger."""
        # add watches
        self.wdp.start()

        super().start()
        logger.debug("Started polling")

    def stop(self):
        """Stop the trigger."""
        super().stop()
        self.wdp.stop()
        self.join()
