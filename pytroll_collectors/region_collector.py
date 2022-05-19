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

"""Region collector."""
import os
from datetime import timedelta, datetime

from pyresample import parse_area_file

try:
    from trollsched.satpass import Pass
except ImportError:
    Pass = None

import logging

logger = logging.getLogger(__name__)


class RegionCollector(object):
    """This is the region collector.

    It collects granules that overlap on a region of interest and return the
    collection of granules when it's done.

    *timeliness* defines the max allowed age of the granule.

    """

    def __init__(self, region,
                 timeliness=None,
                 granule_duration=None,
                 schedule_cut=None,
                 schedule_cut_method=None):
        """Initialize the region collector."""
        self.region = region  # area def
        self.granule_times = set()
        self.granules = []
        self.planned_granule_times = set()
        self.timeliness = timeliness or timedelta(seconds=600)
        self.timeout = None
        self.granule_duration = granule_duration
        self.last_file_added = False
        self.schedule_cut = schedule_cut
        self.schedule_cut_method = schedule_cut_method

    @classmethod
    def from_dict_config(cls, region, config_items):
        """Create a instance of the class using a configuration dictionary to get the parameters."""
        timeliness = timedelta(minutes=int(config_items["timeliness"]))

        try:
            duration = timedelta(seconds=float(config_items["duration"]))
        except KeyError:
            duration = None
        # Parse schedule cut if configured. Mainly for EARS data.
        schedule_cut = config_items.get('schedule_cut')
        # If you want to provide your own method to provide the schedule cut data
        schedule_cut_method = config_items.get('schedule_cut_method')

        return cls(region, timeliness, duration, schedule_cut, schedule_cut_method)

    def __call__(self, granule_metadata):
        """Perform the collection on the granule."""
        try:
            return self.collect(granule_metadata)
        except TypeError:
            raise ImportError("Pytroll-schedule is needed to run RegionCollector")

    def collect(self, granule_metadata):
        """Do the collection."""
        # Check if input data is being waited for

        start_time = granule_metadata['start_time']
        self._set_end_time(granule_metadata)

        logger.debug("Adding area ID %s to metadata for %s",
                     self.region.area_id, _get_platform_name(granule_metadata))
        granule_metadata['collection_area_id'] = self.region.area_id

        self.last_file_added = False
        for ptime in self.planned_granule_times:
            if self._is_new_valid_granule(start_time, ptime):
                self._add_granule(ptime, granule_metadata)
                # If last granule return swath and cleanup
                if self.is_swath_complete():
                    logger.info("Collection finished for %s area %s",
                                _get_platform_name(granule_metadata),
                                str(self.region.area_id))
                    return self.finish()
                self._adjust_timeout()
                return None

        start_time = granule_metadata["start_time"]
        end_time = granule_metadata["end_time"]
        self._set_granule_duration(start_time, end_time)
        logger.debug("Platform name %s and sensor %s: Start and end times = %s %s",
                     str(_get_platform_name(granule_metadata)),
                     str(_get_sensor(granule_metadata)),
                     start_time.strftime('%Y%m%d %H:%M:%S'), end_time.strftime('%Y%m%d %H:%M:%S'))

        if _granule_covers_region(granule_metadata, self.region):
            self._predict_pass_granules(granule_metadata)

        # If last granule return swath and cleanup
        if self.is_swath_complete():
            logger.debug("Collection finished for %s area %s",
                         _get_platform_name(granule_metadata),
                         str(self.region.area_id))
            return self.finish()

        return None

    def _set_end_time(self, granule_metadata):
        if ("end_time" not in granule_metadata and
                self.granule_duration is not None):
            granule_metadata["end_time"] = (granule_metadata["start_time"] +
                                            self.granule_duration)
        granule_metadata['end_time'] = _adjust_end_time(
            granule_metadata['end_time'], granule_metadata["start_time"])

    def is_swath_complete(self):
        """Check if the swath is complete."""
        if self.granule_times:
            if self.planned_granule_times.issubset(self.granule_times):
                return True
            self._adjust_timeout()

        return False

    def _is_new_valid_granule(self, start_time, ptime):
        return (abs(start_time - ptime) < timedelta(seconds=3) and
                ptime not in self.granule_times)

    def _add_granule(self, ptime, granule_metadata):
        self.granule_times.add(ptime)
        self.granules.append(granule_metadata)
        self.last_file_added = True
        logger.info("Added expected granule %s (%s) to area %s",
                    _get_platform_name(granule_metadata),
                    str(granule_metadata["start_time"]),
                    self.region.area_id)

    def _adjust_timeout(self):
        try:
            new_timeout = (max(self.planned_granule_times -
                               self.granule_times) +
                           self.granule_duration +
                           self.timeliness)
        except ValueError:
            logger.error("Calculation of new timeout failed, "
                         "keeping previous timeout.")
            logger.debug("Planned: %s", self.planned_granule_times)
            logger.debug("Received: %s", self.granule_times)
            return
        if new_timeout < self.timeout:
            self.timeout = new_timeout
            logger.info("Adjusted timeout: %s", self.timeout.isoformat())

    def cleanup(self):
        """Clear members."""
        self.granule_times = set()
        self.granules = []
        self.planned_granule_times = set()
        self.timeout = None

    def finish(self):
        """Finish collection, add area ID to metadata, cleanup and return granule metadata."""
        granules = self.granules
        self.cleanup()
        return granules

    def finish_without_reset(self):
        """Finish collection, add area ID to metadata, DON'T cleanup and return granule metadata."""
        return self.granules

    def is_last_file_added(self):
        """Return if last file was added to the region."""
        return self.last_file_added

    def _predict_pass_granules(self, granule_metadata):
        self.granule_times.add(granule_metadata["start_time"])
        self.granules.append(granule_metadata)
        self.last_file_added = True

        # Computation of the predicted granules within the region
        if not self.planned_granule_times:
            self.planned_granule_times.add(granule_metadata["start_time"])
            logger.info("Added new overlapping granule %s (%s) to area %s",
                        _get_platform_name(granule_metadata),
                        str(granule_metadata["start_time"]),
                        self.region.area_id)
            logger.debug("Predicting granules covering %s", self.region.area_id)

            # Forward prediction
            self._predict(granule_metadata, self.granule_duration)
            # Backward prediction
            self._predict(granule_metadata, -self.granule_duration)
            # Check whether schedule should be used
            self._check_schedule(granule_metadata)

            logger.debug("Planned granules for %s over %s: %s",
                         _get_platform_name(granule_metadata),
                         self.region.description,
                         str(sorted(self.planned_granule_times)))
            self.timeout = (max(self.planned_granule_times) +
                            self.granule_duration +
                            self.timeliness)
            logger.info("Planned timeout for %s: %s", self.region.description,
                        self.timeout.isoformat())
        else:
            coverage_str = f"is not overlapping region {self.region.description:s}"
            _log_overlap_message(granule_metadata, coverage_str)

    def _set_granule_duration(self, start_time, end_time):
        if self.granule_duration is None:
            self.granule_duration = end_time - start_time
            logger.debug("Estimated granule duration to %s",
                         str(self.granule_duration))

    def _predict(self, granule_metadata, step):
        gr_time = granule_metadata["start_time"]
        while True:
            gr_time += step
            gr_pass = Pass(_get_platform_name(granule_metadata), gr_time,
                           gr_time + self.granule_duration,
                           instrument=_get_sensor(granule_metadata))
            if not gr_pass.area_coverage(self.region) > 0:
                break
            self.planned_granule_times.add(gr_time)

    def _check_schedule(self, granule_metadata):
        """Check overpass schedule for the satellite and clean the planned granules.

        Sometimes the planned coverage of a pass over the configured region
        is not complete. If you have information of this, this can be harvested
        from such a source like Eumetsat EARS passes. This is the default harvest
        method and source.

        Any other source can be implemented in a method passed in the configuration.
        The method will be pasted a dict (see params below) and the method must return
        two datetimes, minimum and maximum allowed time. The self.planned_granule_times
        will then be modified accordingly.
        """
        if self.schedule_cut:
            method_file_name = "pytroll_collectors.harvest_EUM_schedules"
            name = "harvest_schedules"
            if self.schedule_cut_method:
                logger.debug("Use custom schedule cut method provided in config file...")
                logger.debug("method_name = %s", str(self.schedule_cut_method))
                method_file_name = self.schedule_cut_method
            try:
                logger.debug("Try import {} module: {}".format([name], method_file_name))
                method = __import__(method_file_name, globals(), locals(), [name])
                logger.info("function : {} loaded from module: {}".format([name], method_file_name))
            except ImportError:
                logger.debug("Failed to import schedule_cut for %s from %s. Will not perform schedule cut.",
                             str(name),
                             str(method_file_name))
            else:
                params = {'planned_granule_times': self.planned_granule_times,
                          'granule_metadata': granule_metadata}
                logger.debug("Start harvest of cut schedules")
                logger.debug("method: %s, with type %s", method, type(method))

                min_times, max_times = getattr(method, name)(params)
                logger.debug("From schedule min_times: %s, max_times %s", str(min_times), str(max_times))
                remove_pgt = []
                if min_times is not None and max_times is not None:
                    for pgt in self.planned_granule_times:
                        if pgt < min_times or pgt > max_times:
                            logger.debug("Append to removing list due to schedule cut %s", str(pgt))
                            remove_pgt.append(pgt)
                    for pgt in remove_pgt:
                        self.planned_granule_times.remove(pgt)


def _adjust_end_time(end_time, start_time):
    if start_time > end_time:
        old_end_time = end_time
        end_date = start_time.date()
        if end_time.time() < start_time.time():
            end_date += timedelta(days=1)
        end_time = datetime.combine(end_date, end_time.time())
        logger.debug('Adjusted end time from %s to %s.', old_end_time, end_time)
    return end_time


def _get_platform_name(granule_metadata):
    if "tle_platform_name" in granule_metadata:
        return granule_metadata['tle_platform_name']
    return granule_metadata['platform_name']


def _get_sensor(granule_metadata):
    sensor = granule_metadata["sensor"]
    if isinstance(sensor, list):
        sensor = sensor[0]
    return sensor


def _granule_covers_region(granule_metadata, region):
    granule_pass = Pass(_get_platform_name(granule_metadata),
                        granule_metadata["start_time"],
                        granule_metadata["end_time"],
                        instrument=_get_sensor(granule_metadata))
    coverage = granule_pass.area_coverage(region)
    if coverage > 0:
        coverage_str = f"is overlapping region {region.description:s} by fraction {coverage:.5f}"
        _log_overlap_message(granule_metadata, coverage_str)
        return True
    return False


def _log_overlap_message(granule_metadata, coverage_str):
    try:
        logger.debug(f"Granule {granule_metadata['uri']:s} {coverage_str:s}")
    except KeyError:
        try:
            logger.debug("Granule with start and end times = %s  %s  %s ",
                         str(granule_metadata["start_time"]),
                         str(granule_metadata["end_time"]),
                         coverage_str)
        except KeyError:
            logger.debug("Failed printing debug info...")
            logger.debug("Keys in granule_metadata = %s", str(granule_metadata.keys()))


def get_regions_from_config_dict(config_items):
    """Get the regions from the configuration dictionary."""
    try:
        area_def_file = config_items['area_definition_file']
    except KeyError:
        satpy_config_path = os.environ.get('SATPY_CONFIG_PATH')
        if satpy_config_path is None:
            raise
        area_def_file = os.path.join(satpy_config_path, 'areas.yaml')
    regions = [parse_area_file(area_def_file, region)[0]
               for region in config_items["regions"].split()]
    return regions


def create_collectors_from_config_dict(config_items):
    """Create region collectors for a configuration dictionary."""
    regions = get_regions_from_config_dict(config_items)

    return [RegionCollector.from_dict_config(region, config_items)
            for region in regions]
