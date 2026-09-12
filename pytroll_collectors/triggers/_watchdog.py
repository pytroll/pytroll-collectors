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
