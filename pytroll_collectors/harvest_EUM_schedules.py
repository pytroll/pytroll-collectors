#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2019 - 2021 Pytroll developers
#
# Author(s):
#
#   Trygve Aspenes <trygveas@met.no>
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

"""Harvest EUM schedules. Download schedules from EUM and parse to limit gatherer"""

import re
import os
import tempfile
from datetime import datetime, timedelta
from urllib.request import urlopen
from urllib.error import HTTPError
import logging

logger = logging.getLogger(__name__)

EUM_BASE_URL = 'https://uns.eumetsat.int/downloads/ears/'
download_file = 'ears_{}_pass_prediction_'

eum_platform_name_translate = {'metopa': 'metop-a',
                               'metopb': 'metop-b',
                               'metopc': 'metop-c',
                               'noaa19': 'noaa 19',
                               'noaa20': 'noaa 20',
                               'npp': 'suomi npp',
                               'snpp': 'suomi npp',
                               'fy3d': 'fengyun 3d'}


sensor_translate = {'avhrr/3': 'avhrr',
                    'mersi2': 'mersi'}


def _parse_schedules(params, passes):  # Adam.Dybbroe <a000680@c14526.ad.smhi.se>
    planned_pass_start_time = min(params['planned_granule_times'])
    planned_pass_end_time = max(params['planned_granule_times'])
    planned_pass_mid_time = planned_pass_start_time + (planned_pass_end_time - planned_pass_start_time) / 2

    aos_los = re.compile(r'(\d{4})-(\d{2})-(\d{2})\s(\d{2}):(\d{2}),(\d{4})-(\d{2})-(\d{2})\s(\d{2}):(\d{2}),(.*)')

    min_time = None
    max_time = None
    for pass_ in passes:
        al = aos_los.match(pass_.decode('utf-8'))
        if al:
            eum_aos = datetime(int(al.group(1)), int(al.group(2)), int(al.group(3)),
                               int(al.group(4)), int(al.group(5)))
            eum_los = datetime(int(al.group(6)), int(al.group(7)), int(al.group(8)),
                               int(al.group(9)), int(al.group(10)))
            eum_platform_name = al.group(11)
            platform_name = eum_platform_name_translate.get(eum_platform_name, eum_platform_name)
            if platform_name.upper() != params['granule_metadata']['platform_name'].upper():
                # print("SKipping platform: ", platform_name, params['platform_name'])
                continue
            planned_pass_mid_time = planned_pass_start_time + (planned_pass_end_time - planned_pass_start_time) / 2
            eum_pass_mid_time = eum_aos + (eum_los - eum_aos) / 2
            if abs(eum_pass_mid_time - planned_pass_mid_time) < timedelta(seconds=1000):
                logger.debug("Found pass matching the current planned granule times: %s", str(pass_))
                min_time = eum_aos
                max_time = eum_los
                break

    return (min_time, max_time)


def _generate_pass_list_file_name(params, save_basename, eum_base_url):
    start_time = params['granule_metadata']['start_time']
    if 'sensor' in params['granule_metadata']:
        sensor = params['granule_metadata']['sensor']
        if isinstance(params['granule_metadata']['sensor'], list):
            sensor = params['granule_metadata']['sensor'][0]

        pass_list_file = download_file.format(
            sensor_translate.get(sensor, sensor)) + start_time.strftime('%y-%m-%d') + '.txt'
    else:
        logger.error("sensor not given in params in granule_metadata. Can not continue.")
        return (None, None)
    eum_url = EUM_BASE_URL + pass_list_file
    save_file = os.path.join(save_basename, pass_list_file)
    logger.debug("Pass list save file, %s", save_file)
    return eum_url, save_file


def harvest_schedules(params, save_basename=None, eum_base_url=EUM_BASE_URL):
    if save_basename is None:
        save_basename = tempfile.gettempdir()
    logger.debug("harvest_schedules params: %s", params)

    eum_url, save_file = _generate_pass_list_file_name(params, save_basename, eum_base_url)
    passes = []
    if os.path.exists(save_file):
        with open(save_file, "rb") as fd_:
            logger.debug("Reading from cached files")
            lines = fd_.readlines()
            for line in lines:
                passes.append(line)
    else:
        try:
            logger.debug("EUM_URL, %s", eum_url)
            filedata = urlopen(eum_url)
            passes = filedata.readlines()
        except HTTPError as httpe:
            logger.error("Failed to download file: %s %s", eum_url, httpe)
            return (None, None)
        else:
            with open(save_file, 'wb') as saving_file:
                logger.debug("Saving to file")
                for pass_ in passes:
                    saving_file.write(pass_)

    return _parse_schedules(params, passes)
