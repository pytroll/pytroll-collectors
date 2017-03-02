#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2015, 2017

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
'''

import sys
import os
import time
import logging
from ConfigParser import ConfigParser

from pytroll_collectors.image_scaler import ImageScaler

# TODO: Move to config file
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
