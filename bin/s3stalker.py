#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2020 Martin Raspaud

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
"""S3 stalker."""

from pytroll_collectors import s3stalker
from datetime import datetime, timedelta
from dateutil import tz
import logging
import logging.config
import argparse
from pytroll_collectors.helper_functions import read_yaml
import yaml

LOGGER = logging.getLogger(__name__)


def arg_parse():
    """Handle input arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("bucket", help="The bucket to retrieve from.")
    parser.add_argument("config", help="Config file to be used")
    parser.add_argument("-l", "--log",
                        help="Log configuration file",
                        default=None)

    return parser.parse_args()


if __name__ == '__main__':
    args = arg_parse()

    bucket = args.bucket
    config = read_yaml(args.config)

    if args.log is not None:
        with open(args.log) as fd:
            log_dict = yaml.safe_load(fd.read())
            logging.config.dictConfig(log_dict)

    LOGGER = logging.getLogger('s3stalker')

    time_back = config['timedelta']
    subject = config['subject']

    from posttroll.publisher import Publish
    try:
        with Publish("s3_stalker") as pub:

            s3stalker.set_last_fetch(datetime.now(tz.UTC) - timedelta(**time_back))
            s3_kwargs = config['s3_kwargs']
            fs, files = s3stalker.get_last_files(bucket, **s3_kwargs)
            messages = s3stalker.filelist_unzip_to_messages(fs, files, subject)

            for message in messages:
                LOGGER.info("Publishing %s", str(message))
                pub.send(str(message))

    except KeyboardInterrupt:
        print("terminating publisher...")
