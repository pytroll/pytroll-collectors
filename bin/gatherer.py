#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014 - 2018 PyTroll Community

# Author(s): Martin Raspaud
#            Panu Lahtinen
#            Adam Dybbroe

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

"""Gather granule messages to send them in a bunch."""

from datetime import timedelta
import time
import logging
import logging.handlers
import os
import os.path

from six.moves.configparser import RawConfigParser, NoOptionError

from trollsift import Parser, compose
from pytroll_collectors import trigger
from pytroll_collectors import region_collector
from posttroll import message, publisher
try:
    from satpy.resample import get_area_def
except ImportError:
    from mpop.projector import get_area_def


LOGGER = logging.getLogger(__name__)
CONFIG = RawConfigParser()
PUB = None


def get_metadata(fname):
    """Parse metadata from the file."""
    res = None
    for section in CONFIG.sections():
        try:
            parser = Parser(CONFIG.get(section, "pattern"))
        except NoOptionError:
            continue
        if not parser.validate(fname):
            continue
        res = parser.parse(fname)
        res.update(dict(CONFIG.items(section)))

        for key in ["watcher", "pattern", "timeliness", "regions"]:
            res.pop(key, None)

        res = trigger.fix_start_end_time(res)

        if ("sensor" in res) and ("," in res["sensor"]):
            res["sensor"] = res["sensor"].split(",")

        res["uri"] = fname
        res["filename"] = os.path.basename(fname)

    return res


def terminator(metadata, publish_topic=None):
    """Terminate the gathering."""
    sorted_mda = sorted(metadata, key=lambda x: x["start_time"])

    mda = metadata[0].copy()

    if publish_topic is not None:
        LOGGER.info("Composing topic.")
        subject = compose(publish_topic, mda)
    else:
        LOGGER.info("Using default topic.")
        subject = "/".join(("", mda["format"], mda["data_processing_level"],
                            ''))

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
        msg = message.Message(subject, "collection",
                              mda)
        LOGGER.info("sending %s", str(msg))
        PUB.send(str(msg))
    else:
        LOGGER.warning("Malformed metadata, no key: %s", "uri")


def arg_parse():
    """Handle input arguments."""
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--log",
                        help="File to log to (defaults to stdout)",
                        default=None)
    parser.add_argument("-v", "--verbose", help="print debug messages too",
                        action="store_true")
    parser.add_argument("-c", "--config-item",
                        help="config item to use (all by default). Can be specified multiply times",
                        action="append")
    parser.add_argument("-p", "--publish-port", default=0, type=int,
                        help="Port to publish the messages on. Default: automatic")
    parser.add_argument("-n", "--nameservers",
                        help=("Connect publisher to given nameservers: "
                              "'-n localhost -n 123.456.789.0'. Default: localhost"),
                        action="append")
    parser.add_argument("config", help="config file to be used")

    return parser.parse_args()


def setup(decoder):
    """Set up the granule triggerer."""
    granule_triggers = []

    for section in CONFIG.sections():
        regions = [get_area_def(region)
                   for region in CONFIG.get(section, "regions").split()]

        timeliness = timedelta(minutes=CONFIG.getint(section, "timeliness"))
        try:
            duration = timedelta(seconds=CONFIG.getfloat(section, "duration"))
        except NoOptionError:
            duration = None
        collectors = [region_collector.RegionCollector(region, timeliness, duration)
                      for region in regions]

        try:
            observer_class = CONFIG.get(section, "watcher")
            pattern = CONFIG.get(section, "pattern")
            parser = Parser(pattern)
            glob = parser.globify()
        except NoOptionError:
            observer_class = None

        try:
            publish_topic = CONFIG.get(section, "publish_topic")
        except NoOptionError:
            publish_topic = None

        try:
            nameserver = CONFIG.get(section, "nameserver")
        except NoOptionError:
            nameserver = "localhost"

        try:
            publish_message_after_each_reception = CONFIG.get(section, "publish_message_after_each_reception")
            LOGGER.debug("Publish message after each reception config: {}".format(publish_message_after_each_reception))
        except NoOptionError:
            publish_message_after_each_reception = False

        if observer_class in ["PollingObserver", "Observer"]:
            LOGGER.debug("Using %s for %s", observer_class, section)
            granule_trigger = \
                trigger.WatchDogTrigger(collectors,
                                        terminator,
                                        decoder,
                                        [glob],
                                        observer_class,
                                        publish_topic=publish_topic)

        else:
            LOGGER.debug("Using posttroll for %s", section)
            try:
                duration = CONFIG.getfloat(section, "duration")
            except NoOptionError:
                duration = None

            granule_trigger = trigger.PostTrollTrigger(
                collectors, terminator,
                CONFIG.get(section, 'service').split(','),
                CONFIG.get(section, 'topics').split(','),
                duration=duration,
                publish_topic=publish_topic, nameserver=nameserver,
                publish_message_after_each_reception=publish_message_after_each_reception)
        granule_triggers.append(granule_trigger)

    return granule_triggers


def main():
    """Run the gatherer."""
    global LOGGER
    global PUB

    opts = arg_parse()
    CONFIG.read(opts.config)

    print("Setting timezone to UTC")
    os.environ["TZ"] = "UTC"
    time.tzset()

    handlers = []
    if opts.log:
        handlers.append(logging.handlers.TimedRotatingFileHandler(opts.log,
                                                                  "midnight",
                                                                  backupCount=7))

    handlers.append(logging.StreamHandler())

    if opts.verbose:
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO
    for handler in handlers:
        handler.setFormatter(logging.Formatter("[%(levelname)s: %(asctime)s :"
                                               " %(name)s] %(message)s",
                                               '%Y-%m-%d %H:%M:%S'))
        handler.setLevel(loglevel)
        logging.getLogger('').setLevel(loglevel)
        logging.getLogger('').addHandler(handler)

    logging.getLogger("posttroll").setLevel(logging.INFO)
    LOGGER = logging.getLogger("gatherer")

    if opts.config_item:
        for section in opts.config_item:
            if section not in CONFIG.sections():
                LOGGER.warning(
                    "No config item called %s found in config file.", section)
        for section in CONFIG.sections():
            if section not in opts.config_item:
                CONFIG.remove_section(section)
        if len(CONFIG.sections()) == 0:
            LOGGER.error("No valid config item provided")
            return
        publisher_name = "gatherer_" + "_".join(opts.config_item)
    else:
        publisher_name = "gatherer"

    publish_port = opts.publish_port
    publisher_nameservers = opts.nameservers

    decoder = get_metadata

    PUB = publisher.NoisyPublisher(publisher_name, port=publish_port,
                                   nameservers=publisher_nameservers)

    granule_triggers = setup(decoder)

    PUB.start()

    for granule_trigger in granule_triggers:
        granule_trigger.start()
    try:
        while True:
            time.sleep(1)
            for granule_trigger in granule_triggers:
                if not granule_trigger.is_alive():
                    raise RuntimeError
    except KeyboardInterrupt:
        LOGGER.info("Shutting down...")
    except RuntimeError:
        LOGGER.critical('Something went wrong!')
    except OSError:
        LOGGER.critical('Something went wrong!')
    finally:
        LOGGER.warning('Ending publication the gathering of granules...')
        for granule_trigger in granule_triggers:
            granule_trigger.stop()
        PUB.stop()


if __name__ == '__main__':

    main()
