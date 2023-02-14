# Copyright (c) 2020 - 2023 Pytroll developers

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
"""S3stalker daemon."""
import signal
import time
from datetime import timedelta, datetime, timezone
from threading import Thread

from posttroll.publisher import create_publisher_from_dict_config

from pytroll_collectors.s3stalker import logger, get_last_fetch, create_messages_for_recent_files


class S3StalkerRunner(Thread):
    """Runner for stalking for new files in an S3 object store."""

    def __init__(self, bucket, config, startup_timedelta_seconds):
        """Initialize the S3Stalker runner class."""
        super().__init__()

        self.bucket = bucket
        self.config = config
        self.startup_timedelta_seconds = startup_timedelta_seconds
        self.time_back = self.config['timedelta']
        self._timedelta = self.config['timedelta']
        self._wait_seconds = timedelta(**self.time_back).total_seconds()

        self.publisher = None
        self.loop = False
        self._set_signal_shutdown()

    def _set_signal_shutdown(self):
        """Set a signal to handle shutdown."""
        signal.signal(signal.SIGTERM, self.signal_shutdown)

    def _setup_and_start_communication(self):
        """Set up the Posttroll communication and start the publisher."""
        self.publisher = create_publisher_from_dict_config(self.config['publisher'])
        self.publisher.start()
        self.loop = True

    def signal_shutdown(self, *args, **kwargs):
        """Shutdown the S3 Stalker daemon/runner."""
        self.close()

    def run(self):
        """Start the s3-stalker daemon/runner in a thread."""
        logger.info("Starting up s3stalker.")
        self._setup_and_start_communication()

        first_run = True
        last_fetch_time = None
        while self.loop:
            self._set_timedelta(last_fetch_time, first_run)

            last_fetch_time = get_last_fetch()
            logger.debug("Last fetch time...: %s", str(last_fetch_time))
            first_run = False

            self._process_messages()

            logger.debug("Waiting %d seconds", self._wait_seconds)
            time.sleep(max(self._wait_seconds, 0))

    def _set_timedelta(self, last_fetch_time, first_run):
        self._timedelta = self._get_timedelta(last_fetch_time, is_first_run=first_run)

    def _process_messages(self):
        """Go through all messages in list and publish them one after the other."""
        messages = create_messages_for_recent_files(self.bucket, self.config, self._timedelta)
        for message in messages:
            logger.info("Publishing %s", str(message))
            self.publisher.send(str(message))

    def _get_timedelta(self, last_fetch_time, is_first_run):
        """Get the seconds for the time window to search for (new) files."""
        if is_first_run:
            logger.info('Create messages with urls for most recent files only')
            logger.info('On start up we consider files with age up to %d seconds from now',
                        self.startup_timedelta_seconds)
            return {'seconds': self.startup_timedelta_seconds}

        seconds_back = self._get_seconds_back_to_search(last_fetch_time)
        return {'seconds': seconds_back}

    def _get_seconds_back_to_search(self, last_fetch_time):
        """Update the time to look back considering also the modification time of the last file."""
        if last_fetch_time is None:
            return self._wait_seconds

        start_time = datetime.utcnow()
        start_time = start_time.replace(tzinfo=timezone.utc)
        seconds_to_last_file = (start_time - last_fetch_time).total_seconds()
        return max(self._wait_seconds, seconds_to_last_file)

    def close(self):
        """Shutdown the S3Stalker runner."""
        logger.info('Terminating the S3 Stalker daemon/runner.')
        self.loop = False
        if self.publisher:
            self.publisher.stop()
