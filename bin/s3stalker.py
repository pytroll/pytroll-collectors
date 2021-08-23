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
"""S3 stalker.

This script fetches filenames newer than a given timedelta and publishes the url
of the corresponding file. It exits after that.
So for now, this script is meant to be run at regular intervals, for example
with a cronjob.
"""

import argparse
import logging.config

import yaml

from pytroll_collectors.helper_functions import read_yaml
from pytroll_collectors.s3stalker import publish_new_files


def arg_parse():
    """Handle input arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("bucket", help="The bucket to retrieve from.")
    parser.add_argument("config", help="Config file to be used")
    parser.add_argument("-l", "--log",
                        help="Log configuration file",
                        default=None)

    return parser.parse_args()


def main():
    """Stalk an s3 bucket."""
    args = arg_parse()

    bucket = args.bucket
    config = read_yaml(args.config)

    if args.log is not None:
        with open(args.log) as fd:
            log_dict = yaml.safe_load(fd.read())
            logging.config.dictConfig(log_dict)

    try:
        publish_new_files(bucket, config)
    except KeyboardInterrupt:
        print("terminating publisher...")


if __name__ == '__main__':
    main()
