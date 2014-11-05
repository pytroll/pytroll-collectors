#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014 Martin Raspaud

# Author(s):

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

"""Gather granule messages to send them in a bunch.
"""

from ConfigParser import RawConfigParser, NoOptionError
from trollsift import Parser
from datetime import timedelta, datetime
from trollduction.collectors import trigger
from trollduction.collectors import region_collector
import time
import logging
import os.path
from posttroll import message, publisher
from mpop.projector import get_area_def

logger = logging.getLogger(__name__)

config = RawConfigParser()
pub = None


def get_metadata(fname):
    res = None
    for section in config.sections():
        if section == "default":
            continue
        parser = Parser(config.get(section, "pattern"))
        if not parser.validate(fname):
            continue
        res = parser.parse(fname)
        res.update(dict(config.items(section)))
        del res["pattern"]
        del res["timeliness"]

        if "duration" in res and "end_time" not in res:
            res["end_time"] = (res["start_time"] +
                               timedelta(seconds=int(res["duration"])))
        if "start_date" in res:
            res["start_time"] = datetime.combine(res["start_date"].date(),
                                                 res["start_time"].time())
            if "end_date" not in res:
                res["end_date"] = res["start_date"]
            del res["start_date"]
        if "end_date" in res:
            res["end_time"] = datetime.combine(res["end_date"].date(),
                                               res["end_time"].time())
            del res["end_date"]

        while res["start_time"] > res["end_time"]:
            res["end_time"] += timedelta(days=1)

        if "duration" in res:
            del res["duration"]

        res["uri"] = fname
        res["filename"] = os.path.basename(fname)
    return res


def terminator(metadata):
    """Dummy terminator function.
    """
    sorted_mda = sorted(metadata, key=lambda x: x["start_time"])

    mda = metadata[0].copy()

    subject = "/".join(("",
                        mda["format"],
                        mda["data_processing_level"],
                        ''))
    # TODO: build a collection with some metadata, strip out the datasets.

    mda['end_time'] = sorted_mda[-1]['end_time']

    mda['collection'] = []

    for meta in sorted_mda:
        new_mda = {}
        for key in ['dataset', 'uri', 'uid']:
            if key in meta:
                new_mda[key] = meta[key]
            new_mda['start_time'] = meta['start_time']
            new_mda['end_time'] = meta['end_time']
        mda['collection'].append(new_mda)

    for key in ['dataset', 'uri', 'uid']:
        if key in mda:
            del mda[key]

    msg = message.Message(subject, "collection",
                          mda)
    logger.info("sending %s", str(msg))
    pub.send(str(msg))


if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--log", help="File to log to (defaults to stdout)",
                        default=None)
    parser.add_argument("-v", "--verbose", help="print debug messages too",
                        action="store_true")
    parser.add_argument("config", help="config file to be used")
    opts = parser.parse_args()

    config.read(opts.config)

    if opts.log:
        import logging.handlers
        handler = logging.handlers.TimedRotatingFileHandler(opts.log,
                                                            "midnight",
                                                            backupCount=7)
    else:
        handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[%(levelname)s: %(asctime)s :"
                                           " %(name)s] %(message)s",
                                           '%Y-%m-%d %H:%M:%S'))
    if opts.verbose:
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO
    handler.setLevel(loglevel)
    logging.getLogger('').setLevel(loglevel)
    logging.getLogger('').addHandler(handler)
    logging.getLogger("posttroll").setLevel(logging.INFO)
    logger = logging.getLogger("gatherer")

    decoder = get_metadata

    granule_triggers = []

    pub = publisher.NoisyPublisher("eumetcast_poller")

    # TODO: get this from the product config files
    regions = [get_area_def(region)
               for region in config.get("default", "regions").split()]

    for section in config.sections():
        if section == "default":
            continue

        timeliness = timedelta(minutes=config.getint(section, "timeliness"))
        try:
            duration = timedelta(seconds=config.getfloat(section, "duration"))
        except NoOptionError:
            duration = None
        collectors = [region_collector.RegionCollector(
            region, timeliness, duration) for region in regions]

        try:
            pattern = config.get(section, "pattern")
            parser = Parser(pattern)

            granule_trigger = trigger.WatchDogTrigger(collectors, terminator,
                                                      decoder, [parser.globify()])
        except NoOptionError:
            granule_trigger = trigger.PostTrollTrigger(
                collectors, terminator,
                config.get(section, 'service').split(','),
                config.get(section, 'topics').split(','))
        granule_triggers.append(granule_trigger)

    pub.start()
    for granule_trigger in granule_triggers:
        granule_trigger.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        for granule_trigger in granule_triggers:
            granule_trigger.stop()
        pub.stop()
