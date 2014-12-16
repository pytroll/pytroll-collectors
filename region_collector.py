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

"""
"""

import os
import time
from datetime import timedelta, datetime

import pyproj
from glob import glob

import numpy as np
from pyresample import geometry, utils
from pyorbital.orbital import Orbital
#from orbitno import get_tle

import logging

LOG = logging.getLogger(__name__)

PLOT = False

tle_names = {"metop a": "metop-a",
             "metop b": "metop-b",
             "suomi-npp": "suomi npp",
             "noaa-19": "noaa19"}


def corners(platform, start_time, end_time):
    """ Compute the corners of a satellite granule

        returns area (BaseDefinition)
    """
    #tle = get_tle(platform, start_time)

    orbital = Orbital(platform)
    track_start = orbital.get_lonlatalt(start_time)
    track_end = orbital.get_lonlatalt(end_time)

    geod = pyproj.Geod(ellps='WGS84')
    az_fwd, az_back, dist = geod.inv(track_start[0], track_start[1],
                                     track_end[0], track_end[1])

    del dist

    NPP_WIDTH = 3000000

    start_left = geod.fwd(track_start[0], track_start[1],
                          az_fwd - 90, NPP_WIDTH / 2)
    start_right = geod.fwd(track_start[0], track_start[1],
                           az_fwd + 90, NPP_WIDTH / 2)
    end_left = geod.fwd(
        track_end[0], track_end[1], az_back + 90, NPP_WIDTH / 2)
    end_right = geod.fwd(
        track_end[0], track_end[1], az_back - 90, NPP_WIDTH / 2)

    # Check in input data is within the region

    lons = np.array([[start_left[0], end_left[0]],
                     [start_right[0], end_right[0]]])
    lats = np.array([[start_left[1], end_left[1]],
                     [start_right[1], end_right[1]]])

    granule_area = geometry.SwathDefinition(lons, lats)
    return granule_area


class RegionCollector:

    """This is the region collector. It collects granules that overlap on a
    region of interest and return the collection of granules when it's done.

    *timeliness* defines the max allowed age of the granule.

    """

    def __init__(self, region,
                 timeliness=timedelta(seconds=600),
                 granule_duration=None):
        self.region = region  # area def
        self.granule_times = set()
        self.granules = []
        self.planned_granule_times = set()
        self.timeliness = timeliness
        self.timeout = None
        self.granule_duration = granule_duration

    def __call__(self, granule_metadata):
        return self.collect(granule_metadata)

    def collect(self, granule_metadata):
        """ 
            Parameters:

                granule_metadata : metadata 

        """

        # Check if input data is being waited for

        platform = granule_metadata['platform_name']
        fullname = platform
        if 'number' in granule_metadata:
            number = granule_metadata['number']
            fullname = tle_names.get(fullname + " " + number,
                                     fullname + " " + number)
        else:
            fullname = tle_names.get(fullname.lower(),
                                     fullname.lower())
        start_time = granule_metadata['start_time']
        end_time = granule_metadata['end_time']

        for ptime in self.planned_granule_times:
            if abs(start_time - ptime) < timedelta(seconds=3):
                self.granule_times.add(ptime)
                self.granules.append(granule_metadata)
                LOG.info("Added %s (%s) granule to area %s",
                         platform,
                         str(start_time),
                         self.region.area_id)
                # If last granule return swath and cleanup
                if self.granule_times == self.planned_granule_times:
                    LOG.info("Collection finished for area: " +
                             str(self.region.area_id))
                    return self.finish()
                else:
                    return

        # Get corners from input data

        if self.granule_duration is None:
            self.granule_duration = end_time - start_time
            LOG.debug("Estimated granule duration to " +
                      str(self.granule_duration))

        granule_area = corners(fullname, start_time, end_time)

        # If file is within region, make pass prediction to know what to wait
        # for
        if granule_area.overlaps(self.region):
            self.granule_times.add(start_time)
            self.granules.append(granule_metadata)

            # Computation of the predicted granules within the region

            if not self.planned_granule_times:
                self.planned_granule_times.add(start_time)
                LOG.info("Added %s (%s) granule to area %s",
                         platform,
                         str(start_time),
                         self.region.area_id)
                LOG.debug(
                    "Predicting granules covering " + self.region.area_id)
                gr_time = start_time
                while True:
                    gr_time += self.granule_duration
                    gr_area = corners(fullname, gr_time,
                                      gr_time + self.granule_duration)
                    if not gr_area.overlaps(self.region):
                        break
                    self.planned_granule_times.add(gr_time)

                gr_time = start_time
                while True:
                    gr_time -= self.granule_duration
                    gr_area = corners(fullname, gr_time,
                                      gr_time + self.granule_duration)
                    if not gr_area.overlaps(self.region):
                        break
                    self.planned_granule_times.add(gr_time)

                LOG.info(
                    "Planned granules: " + str(sorted(self.planned_granule_times)))
                self.timeout = (max(self.planned_granule_times)
                                + self.granule_duration
                                + self.timeliness)
                LOG.info("Planned timeout: " + self.timeout.isoformat())
        else:
            try:
                LOG.debug("Granule %s is not overlapping %s",
                          granule_metadata["uri"], self.region.name)
            except KeyError:
                try:
                    LOG.debug("Granule with start and end times = " +
                              str(granule_metadata["start_time"]) + " " +
                              str(granule_metadata["end_time"]) +
                              "is not overlapping " + str(self.region.name))
                except KeyError:
                    LOG.debug("Failed printing debug info...")
                    LOG.debug("Keys in granule_metadata = " +
                              str(granule_metadata.keys()))

        # If last granule return swath and cleanup
        if (self.granule_times and
                (self.granule_times == self.planned_granule_times)):
            LOG.debug("Collection finished for area: " +
                      str(self.region.area_id))
            return self.finish()

    def cleanup(self):
        self.granule_times = set()
        self.granules = []
        self.planned_granule_times = set()
        self.timeout = None

    def finish(self):
        granules = self.granules
        self.cleanup()
        return granules


def read_granule_metadata(filename):
    """ """
    import json
    with open(filename) as jfp:
        metadata = json.load(jfp)[0]

    metadata['uri'] = "file://" + os.path.abspath(filename)

    for attr in ["start_time", "end_time"]:
        try:
            metadata[attr] = datetime.strptime(
                metadata[attr], "%Y-%m-%dT%H:%M:%S.%f")
        except ValueError:
            metadata[attr] = datetime.strptime(
                metadata[attr], "%Y-%m-%dT%H:%M:%S")
    return metadata


if __name__ == '__main__':

    LOG.info("Welcome to pytroll")

    input_dir = "tests/data"

    region = utils.load_area(
        '/home/a001673/usr/src/satprod/etc/areas.def', 'scan1')
    LOG.debug("Read area " + str(region))

    NPP_GRANULE_DURATION = timedelta(seconds=85.75)

    time_diff = datetime.utcnow() - datetime(2014, 10, 17, 11, 34, 39)

    collector = RegionCollector(region,
                                timeliness=time_diff + timedelta(seconds=5),
                                granule_duration=NPP_GRANULE_DURATION)
    old_granules = set()

    try:
        while True:

            input_granules = set(glob(os.path.join(input_dir, "npp*.json")))

            input_granules -= old_granules

            swath = None

            for input_granule in sorted(list(input_granules)):
                metadata = read_granule_metadata(input_granule)
                swath = swath or collector.collect(metadata)
                old_granules.add(input_granule)

            if swath is not None:
                break

            if (collector.timeout is not None) and datetime.utcnow() > collector.timeout:
                LOG.warning("Timeout! (" + collector.timeout.isoformat() + ")")
                swath = collector.granules
                break

            time.sleep(1)

    except KeyboardInterrupt:
        pass

    print swath
