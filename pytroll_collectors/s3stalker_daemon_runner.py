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
from datetime import timedelta, datetime
from threading import Thread

from dateutil.tz import UTC

from posttroll.publisher import create_publisher_from_dict_config

from pytroll_collectors.s3stalker import logger, get_last_fetch, create_messages_for_recent_files, set_last_fetch


class S3StalkerRunner(Thread):
    """Runner for stalking for new files in an S3 object store."""

    def __init__(self, bucket, config):
        """Initialize the S3Stalker runner class."""
        super().__init__()

        self.bucket = bucket
        self.config = config
        startup_time = timedelta(**self.config.pop("fetch_back_to"))
        self.startup_timedelta_seconds = startup_time.total_seconds()
        self.time_back = self.config.pop('polling_interval')
        self._timedelta = self.time_back
        self._wait_seconds = timedelta(**self.time_back).total_seconds()

        self.publisher = None
        self.loop = False
        self._set_signal_shutdown()
        last_fetch_time = datetime.now(UTC) - startup_time
        set_last_fetch(last_fetch_time)

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

        while self.loop:

            last_fetch_time = get_last_fetch()
            logger.debug("Last fetch time...: %s", str(last_fetch_time))

            self._fetch_bucket_content_and_publish_new_files()

            logger.debug("Waiting %d seconds", self._wait_seconds)
            time.sleep(max(self._wait_seconds, 0))

    def _fetch_bucket_content_and_publish_new_files(self):
        """Go through all messages in list and publish them one after the other."""
        messages = create_messages_for_recent_files(self.bucket, self.config)
        for message in messages:
            logger.info("Publishing %s", str(message))
            self.publisher.send(str(message))

    def close(self):
        """Shutdown the S3Stalker runner."""
        logger.info('Terminating the S3 Stalker daemon/runner.')
        self.loop = False
        if self.publisher:
            self.publisher.stop()
