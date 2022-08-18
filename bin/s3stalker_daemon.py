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
from datetime import datetime, timedelta, timezone
import time
import yaml
import sys

from pytroll_collectors.helper_functions import read_yaml
from pytroll_collectors.s3stalker import create_messages_for_recent_files
from pytroll_collectors.s3stalker import get_last_fetch

import signal
from threading import Thread
from posttroll.publisher import create_publisher_from_dict_config

logger = logging.getLogger(__name__)


class S3StalkerRunner(Thread):
    """Runner for stalking for new files in an S3 object store."""

    def __init__(self, bucket, config, startup_timedelta_seconds):
        """Initialize the S3Stalker runner class."""
        super().__init__()

        self.bucket = bucket
        self.config = config
        self.startup_timedelta_seconds = startup_timedelta_seconds
        self.time_back = self.config['timedelta']
        self.publisher = None
        self.loop = False
        self._setup_and_start_communication()

    def _setup_and_start_communication(self):
        """Set up the Posttroll communication and start the publisher."""
        logger.debug("Starting up... ")
        self.publisher = create_publisher_from_dict_config(self.config['publisher'])
        self.publisher.start()
        self.loop = True
        signal.signal(signal.SIGTERM, self.signal_shutdown)

    def signal_shutdown(self, *args, **kwargs):
        """Shutdown the S3 Stalker daemon/runner."""
        self.close()

    def run(self):
        """Start the s3-stalker daemon/runner in a thread."""
        first_run = True
        config = self.config
        last_fetch_time = None
        while self.loop:
            if first_run:
                logger.info('Create messages with urls for most recent files only')
                logger.info('On start up we consider files with age up to %d seconds from now',
                            self.startup_timedelta_seconds)
                config['timedelta'] = {'seconds': self.startup_timedelta_seconds}
                first_run = False
            else:
                seconds_back = self._get_seconds_back_to_search(last_fetch_time)
                config['timedelta'] = {'seconds': seconds_back}

            messages = create_messages_for_recent_files(self.bucket, config)

            last_fetch_time = get_last_fetch()
            logger.info("Last fetch time...: %s", str(last_fetch_time))

            for message in messages:
                logger.info("Publishing %s", str(message))
                self.publisher.send(str(message))

            waiting_time = timedelta(**self.time_back)
            wait_seconds = waiting_time.total_seconds()

            # Wait for some time...
            logger.debug("Waiting %d seconds", wait_seconds)
            time.sleep(max(wait_seconds, 0))

    def _get_seconds_back_to_search(self, last_fetch_time):
        """Update the time to look back considering also the modification time of the last file."""
        dtime_back = timedelta(**self.time_back)
        seconds_back = dtime_back.total_seconds()

        if last_fetch_time is None:
            return seconds_back

        start_time = datetime.utcnow()
        start_time = start_time.replace(tzinfo=timezone.utc)
        seconds_to_last_file = (start_time - last_fetch_time).total_seconds()
        return max(seconds_back, seconds_to_last_file)

    def close(self):
        """Shutdown the S3Stalker runner."""
        logger.info('Terminating the S3 Stalker daemon/runner.')
        self.loop = False
        if self.publisher:
            try:
                self.publisher.stop()
            except Exception:
                logger.exception("Couldn't stop publisher.")


def arg_parse():
    """Handle input arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("bucket", help="The bucket to retrieve from.")
    parser.add_argument("config", help="Config file to be used")
    parser.add_argument("-s", "--startup_timedelta_seconds",
                        type=int,
                        help="The time window in seconds back in time on start up (default=3600)",
                        default=3600)
    parser.add_argument("-l", "--log",
                        help="Log configuration file",
                        default=None)

    return parser.parse_args()


def main():
    """Stalk an s3 bucket."""
    args = arg_parse()

    bucket = args.bucket
    config = read_yaml(args.config)
    startup_timedelta_seconds = args.startup_timedelta_seconds

    if args.log is not None:
        with open(args.log) as fd:
            log_dict = yaml.safe_load(fd.read())
            logging.config.dictConfig(log_dict)

    logger.info("Try start the s3-stalker runner:")
    try:
        s3runner = S3StalkerRunner(bucket, config, startup_timedelta_seconds)

    except Exception as err:
        logger.error('The S3 Stalker Runner crashed: %s', str(err))
        sys.exit(1)
    try:
        s3runner.start()
        s3runner.join()
    except KeyboardInterrupt:
        logger.debug("Interrupting")
    finally:
        s3runner.close()


if __name__ == '__main__':
    main()
