#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2012, 2014, 2015 Martin Raspaud

# Author(s):

#   Kristian Rune Larsen <krl@dmi.dk>
#   Martin Raspaud <martin.raspaud@smhi.se>
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

"""Triggers for region_collectors
"""

from pyinotify import (ProcessEvent, Notifier, WatchManager,
                       IN_CLOSE_WRITE, IN_MOVED_TO)
import logging
from datetime import datetime
from fnmatch import fnmatch
import os.path
from posttroll.subscriber import NSSubscriber


LOG = logging.getLogger(__name__)


def total_seconds(tdef):
    """Calculate total time in seconds.
    """
    return ((tdef.microseconds +
             (tdef.seconds + tdef.days * 24 * 3600) * 10 ** 6) / 10.0 ** 6)


class Trigger(object):

    """Abstract trigger class.
    """

    def __init__(self, collectors, terminator, publish_topic=None):
        self.collectors = collectors
        self.terminator = terminator
        self.publish_topic = publish_topic

    def _do(self, metadata):
        """Execute the collectors and terminator.
        """
        if not metadata:
            LOG.warning("No metadata")
            return
        for collector in self.collectors:
            res = collector(metadata.copy())
            if res:
                return self.terminator(res, publish_topic=self.publish_topic)


from threading import Thread, Event


class FileTrigger(Trigger, Thread):

    """File trigger, acting upon inotify events.
    """

    def __init__(self, collectors, terminator, decoder, publish_topic=None):
        Thread.__init__(self)
        Trigger.__init__(self, collectors, terminator,
                         publish_topic=publish_topic)
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
                    LOG.debug("Area: %s, timeout: %s",
                              next_timeout[0].region,
                              str(next_timeout[1]))
                    self.terminator(next_timeout[0].finish(),
                                    publish_topic=self.publish_topic)
                else:
                    LOG.debug("Waiting %s seconds until timeout",
                              str(total_seconds(next_timeout[1] -
                                                datetime.utcnow())))
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

    def __init__(self, collectors, terminator, decoder, patterns,
                 publish_topic=None):
        ProcessEvent.__init__(self)
        FileTrigger.__init__(self, collectors, terminator, decoder,
                             publish_topic=publish_topic)
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
                LOG.debug("New file detected (close write): %s",
                          event.pathname)
                self.add_file(event.pathname)

    def process_IN_MOVED_TO(self, event):
        """On moving a file into the directory.
        """
        for pattern in self.patterns:
            if fnmatch(event.src_path, pattern):
                LOG.debug("New file detected (moved to): %s",
                          event.pathname)
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
    from watchdog.observers import Observer

    class WatchDogTriggerOld(FileSystemEventHandler, FileTrigger):

        """File trigger, acting upon inotify events.
        """

        cases = {"PollingObserver": PollingObserver,
                 "Observer": Observer}

        def __init__(self, collectors, terminator, decoder, patterns,
                     observer_class_name, publish_topic=None):
            FileSystemEventHandler.__init__(self)
            FileTrigger.__init__(self, collectors, terminator, decoder,
                                 publish_topic=publish_topic)
            self.input_dirs = []
            for pattern in patterns:
                self.input_dirs.append(os.path.dirname(pattern))
            self.patterns = patterns

            self.new_file = Event()
            self.observer = self.cases.get(observer_class_name, Observer)()

        def on_created(self, event):
            """On creating a file.
            """
            try:
                for pattern in self.patterns:
                    if fnmatch(event.src_path, pattern):
                        LOG.debug("New file detected (created): %s",
                                  event.src_path)
                        self.add_file(event.src_path)
                        LOG.debug("Done adding")
                        return
            except Exception as e:
                LOG.exception(
                    "Something wrong happened in the event processing: %s",
                    e.message)

        def start(self):
            """Start trigger.
            """
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

    class AbstractWatchDogProcessor(FileSystemEventHandler):

        """File trigger, acting upon file system events.
        """

        cases = {"PollingObserver": PollingObserver,
                 "Observer": Observer}

        def __init__(self, patterns, observer_class_name="Observer"):
            FileSystemEventHandler.__init__(self)
            self.input_dirs = []
            for pattern in patterns:
                self.input_dirs.append(os.path.dirname(pattern))
                LOG.debug("watching " + str(os.path.dirname(pattern)))
            self.patterns = patterns

            self.new_file = Event()
            self.observer = self.cases.get(observer_class_name, Observer)()

        def on_created(self, event):
            """On creating a file.
            """
            try:
                for pattern in self.patterns:
                    if fnmatch(event.src_path, pattern):
                        LOG.debug(
                            "New file detected (created): " + event.src_path)
                        self.process(event.src_path)
                        LOG.debug("Done processing file")
                        return
            except:
                LOG.exception(
                    "Something wrong happened in the event processing!")

        def process(self, pathname):
            raise NotImplementedError

        def start(self):
            """Start processor"""
            # add watches
            for idir in self.input_dirs:
                self.observer.schedule(self, idir)
            self.observer.start()

            LOG.debug("Started watching filesystem")

        def stop(self):
            """Stop processor"""
            self.observer.stop()
            self.observer.join()

    class WatchDogTrigger(FileTrigger):

        """File trigger, acting upon filesystem events.
        """

        def __init__(self, collectors, terminator, decoder,
                     patterns, observer_class_name, publish_topic=None):
            self.wdp = AbstractWatchDogProcessor(patterns, observer_class_name)
            FileTrigger.__init__(self, collectors, terminator, decoder,
                                 publish_topic=publish_topic)
            self.wdp.process = self.add_file

        def start(self):

            # add watches
            self.wdp.start()

            FileTrigger.start(self)
            LOG.debug("Started polling")

        def stop(self):
            FileTrigger.stop(self)
            self.wdp.stop()
            self.join()


except ImportError:
    LOG.error("Watchdog import failed!")
    WatchDogTrigger = None


class AbstractMessageProcessor(Thread):

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

    def process(self, msg):
        """Process the message.
        """
        del msg
        raise NotImplementedError("process is not implemented!")

    def run(self):
        """Run the trigger.
        """
        try:
            for msg in self.sub.recv(2):
                if not self.loop:
                    break
                if msg is None:
                    continue
                self.process(msg)
        finally:
            self.stop()

    def stop(self):
        """Stop the trigger.
        """
        self.nssub.stop()
        self.loop = False


class PostTrollTrigger(FileTrigger):

    """Get posttroll messages.
    """

    def __init__(self, collectors, terminator, services, topics,
                 publish_topic=None):
        self.msgproc = AbstractMessageProcessor(services, topics)
        self.msgproc.process = self.add_file
        FileTrigger.__init__(self, collectors, terminator, self.decode_message,
                             publish_topic=publish_topic)

    def start(self):
        """Start the posttroll trigger.
        """
        FileTrigger.start(self)
        self.msgproc.start()

    @staticmethod
    def decode_message(message):
        """Return the message data.
        """
        return message.data

    def stop(self):
        """Stop the posttroll trigger.
        """
        self.msgproc.stop()
        FileTrigger.stop(self)
