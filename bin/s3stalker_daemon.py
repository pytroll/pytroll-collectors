#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2022 Pytroll developers

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

"""S3stalker daemon/runner.

This is a daemon supposed to stay up and running "forever". It will continously
fetch fresh filenames from an S3 object store and publish the urls of those new
filenames. It is a daemon version of the s3stalker.py script which needs to be
run as a cronjob.

"""

import argparse
import logging
import logging.config
import yaml
import sys

from pytroll_collectors.helper_functions import read_yaml
from pytroll_collectors.s3stalker_daemon_runner import S3StalkerRunner

logger = logging.getLogger(__name__)


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

    logger.info("Try start the s3-stalker runner:")
    try:
        s3runner = S3StalkerRunner(bucket, config)
        s3runner.start()
        s3runner.join()
    except KeyboardInterrupt:
        logger.debug("Interrupting")
    except Exception as err:
        logger.error('The S3 Stalker Runner crashed: %s', str(err))
        sys.exit(1)
    finally:
        s3runner.close()


if __name__ == '__main__':
    main()
