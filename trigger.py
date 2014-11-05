#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2012, 2014 Martin Raspaud

# Author(s):

#   Kristian Rune Larsen <krl@dmi.dk>
#   Martin Raspaud <martin.raspaud@smhi.se>

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

"""Triggers for region_collectors
"""

from pyinotify import (ProcessEvent, Notifier, WatchManager,
                       IN_CLOSE_WRITE, IN_MOVED_TO)
import logging
from datetime import datetime
from posttroll.subscriber import NSSubscriber
LOG = logging.getLogger(__name__)


def total_seconds(td):
    return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10 ** 6) / 10 ** 6


class Trigger:

    """Abstract trigger class.
    """

    def __init__(self, collectors, terminator):
        self.collectors = collectors
        self.terminator = terminator

    def _do(self, metadata):
        """Execute the collectors and terminator.
        """
        if not metadata:
            return
        for collector in self.collectors:
            res = collector(metadata)
            if res:
                return self.terminator(res)


from threading import Thread, Event


class FileTrigger(Trigger, Thread):

    """File trigger, acting upon inotify events.
    """

    def __init__(self, collectors, terminator, decoder):
        Thread.__init__(self)
        Trigger.__init__(self, collectors, terminator)
        self.decoder = decoder
        self._running = True
        self.new_file = Event()

    def _do(self, pathname):
        mda = self.decoder(pathname)
        LOG.debug("mda: %s", str(mda))
        Trigger._do(self, mda)

    def add_file(self, pathname):
        """On arrival of a file.
        """
        self._do(pathname)
        self.new_file.set()

    def run(self):
        """The timeouts are handled here.
        """
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
                next_timeout = min(timeouts, key=(lambda(x): x[1]))
                if next_timeout[1] and (next_timeout[1] < datetime.utcnow()):
                    LOG.warning("Timeout detected, terminating collector")
                    self.terminator(next_timeout[0].finish())
                else:
                    self.new_file.wait(total_seconds(next_timeout[1] -
                                                     datetime.utcnow()))
                    self.new_file.clear()
            else:
                self.new_file.wait()
                self.new_file.clear()

    def stop(self):
        """Stopping everything.
        """
        self._running = False
        self.new_file.set()


class InotifyTrigger(ProcessEvent, FileTrigger):

    """File trigger, acting upon inotify events.
    """

    def __init__(self, collectors, terminator, decoder, patterns):
        ProcessEvent.__init__(self)
        FileTrigger.__init__(self, collectors, terminator, decoder)
        self.input_dirs = []
        for pattern in patterns:
            self.input_dirs.append(os.path.dirname(pattern))
        self.patterns = patterns
        self.new_file = Event()

    def process_IN_CLOSE_WRITE(self, event):
        """On closing a file.
        """
        for pattern in self.patterns:
            if fnmatch(event.src_path, pattern):
                LOG.debug("New file detected (close write): " + event.pathname)
                self.add_file(event.pathname)

    def process_IN_MOVED_TO(self, event):
        """On moving a file into the directory.
        """
        for pattern in self.patterns:
            if fnmatch(event.src_path, pattern):
                LOG.debug("New file detected (moved to): " + event.pathname)
                self.add_file(event.pathname)

    def loop(self):
        """The main function.
        """
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

try:
    from watchdog.events import FileSystemEventHandler
    from watchdog.observers.polling import PollingObserver
except ImportError:
    FileSystemEventHandler = object
    PollingObserver = object

import time
from fnmatch import fnmatch
import os.path


class WatchDogTrigger(FileSystemEventHandler, FileTrigger):

    """File trigger, acting upon inotify events.
    """

    def __init__(self, collectors, terminator, decoder, patterns):
        FileSystemEventHandler.__init__(self)
        FileTrigger.__init__(self, collectors, terminator, decoder)
        self.input_dirs = []
        for pattern in patterns:
            self.input_dirs.append(os.path.dirname(pattern))
        self.patterns = patterns

        self.new_file = Event()
        self.observer = None

    def on_created(self, event):
        """On creating a file.
        """
        for pattern in self.patterns:
            if fnmatch(event.src_path, pattern):
                LOG.debug("New file detected (created): " + event.src_path)
                self.add_file(event.src_path)
                return

    def start(self):
        self.observer = PollingObserver()

        # add watches
        for idir in self.input_dirs:
            self.observer.schedule(self, idir)
        self.observer.start()

        FileTrigger.start(self)
        LOG.debug("Started polling")

    def stop(self):
        self.observer.stop()
        FileTrigger.stop(self)
        self.observer.join()
        self.join()

    # def loop(self):
    #     """The main function.
    #     """
    #     self.start()
    #     try:
    #         observer = PollingObserver()

    # add watches
    #         for idir in self.input_dirs:
    #             observer.schedule(self, idir)
    #         observer.start()
    #         LOG.debug("Started")
    # loop forever
    #         while self._running:
    #             time.sleep(1)
    #     finally:
    #         observer.stop()
    #         observer.join()
    #         self.stop()
    #         self.join()


class MessageProcessor(Thread):

    """Process Messages
    """

    def __init__(self, services, topics):
        Thread.__init__(self)
        self.nssub = NSSubscriber(services, topics, True)
        self.sub = None
        self.loop = True

    def start(self):
        self.sub = self.nssub.start()
        Thread.start(self)

    def process_message(self, msg):
        del msg
        raise NotImplementedError("process_message is not implemented!")

    def run(self):
        try:
            for msg in self.sub.recv(2):
                if not self.loop:
                    break
                if msg is None:
                    continue
                self.process_message(msg)
        finally:
            self.stop()

    def stop(self):
        self.nssub.stop()
        self.loop = False


class PostTrollTrigger(MessageProcessor, FileTrigger):

    """Get posttroll messages.
    """

    def __init__(self, collectors, terminator, services, topics):
        MessageProcessor.__init__(self, services, topics)
        FileTrigger.__init__(self, collectors, terminator, self.decode_message)

    @staticmethod
    def decode_message(message):
        return message.data

    def stop(self):
        MessageProcessor.stop(self)
        FileTrigger.stop(self)

    def process_message(self, msg):
        self.add_file(msg)
