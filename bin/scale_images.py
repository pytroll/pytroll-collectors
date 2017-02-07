#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2015

# Author(s):

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

'''Listen to messages and generate smaller versions of the images.

Example scale_images.ini:

[/HRPT/L3/dev/hki/euron1]
# <x_size>x<y_size>+<x_start>+<y_start>
crops = ,768x768+800+300,
sizes = 3072x3072,768x768,384x384
tags = large,small,thumb
out_dir = /tmp
areaname = euron1
in_pattern = {time:%Y%m%d_%H%M}_Metop-A_{areaname}_{composite}.png
out_dir = /lustre/tmp/data/oper/test/qlook/{tag}
out_pattern = {time:%Y%m%d_%H%M}_MetopA-{composite}-{areaname}.png
overlay_config = /home/users/satman/config_files/pycoast_euron1_overlay_config.ini
use_platform_name_hack = False
timeliness = 10
latest_composite_image = /tmp/latest_polar_data-{composite}-{areaname}.png
'''

import sys
import os
import time
import logging
from ConfigParser import ConfigParser

from pytroll_collectors.image_scaler import ImageScaler

GSHHS_DATA_ROOT = os.environ['GSHHS_DATA_ROOT']

LOG_CONFIG = {'version': 1,
              'handlers': {
                  'console':  {
                      'class': 'logging.StreamHandler',
                      'level': 'DEBUG',
                      'formatter': 'simple',
                      'stream': sys.stdout,
                  },
                  'file': {
                      'class': 'logging.handlers.TimedRotatingFileHandler',
                      'level': 'DEBUG',
                      'formatter': 'simple',
                      'filename': '/tmp/scale_images.log',
                      'backupCount': 7,
                      'when': 'midnight',
                      'utc': True,
                  }
              },
              'formatters': {
                  'simple': {
                      'format': '[%(levelname)s: %(asctime)s] %(message)s',
                  }
              },
              'loggers': {
                  '': {
                      'handlers': ['console', 'file'],
                      'level': 'DEBUG',
                      'propagate': True
                  }
              }
              }


def main():
    '''Main'''

    os.environ["TZ"] = "UTC"
    time.tzset()

    config_file = sys.argv[1]
    config = ConfigParser()
    config.read(config_file)

    logging.config.dictConfig(LOG_CONFIG)

    logging.info("Config read")

    scaler = ImageScaler(config)

    try:
        logging.info("Starting ImageScaler")
        scaler.run()
    except KeyboardInterrupt:
        logging.info("Stopping ImageScaler")
        scaler.stop()


if __name__ == "__main__":
    main()
