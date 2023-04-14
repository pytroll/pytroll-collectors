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
"""S3stalker daemon.

The contents of the yaml configuration file should look like this::

    s3_kwargs:
      anon: false
      client_kwargs:
        aws_access_key_id: my_accesskey
        aws_secret_access_key: my_secret_key
        endpoint_url: https://xxx.yyy.zz
    fetch_back_to:
      hours: 20
    polling_interval:
      minutes: 2
    file_pattern: '{platform_name:3s}_OL_2_{datatype_id:_<6s}_{start_time:%Y%m%dT%H%M%S}_{end_time:%Y%m%dT%H%M%S}_{creation_time:%Y%m%dT%H%M%S}_{duration:4d}_{cycle:3d}_{relative_orbit:3d}_{frame:4d}_{centre:3s}_{mode:1s}_{timeliness:2s}_{collection:3s}.zip'
    subject: /segment/2/safe-olci/S3/
    publisher:
      name: s3stalker_runner
"""  # noqa
import signal
from datetime import timedelta, datetime
from threading import Thread

from dateutil.tz import UTC

from posttroll.publisher import create_publisher_from_dict_config

from pytroll_collectors.s3stalker import logger, create_messages_for_recent_files, set_last_fetch, sleeper


class S3StalkerRunner(Thread):
    """Runner for stalking for new files in an S3 object store."""

    def __init__(self, bucket, config, publisher_ready_time=2.5):
        """Initialize the S3Stalker runner class."""
        super().__init__()

        self.bucket = bucket
        fetch_back_time = timedelta(**config.pop("fetch_back_to"))

        self._wait_seconds = timedelta(**config.pop('polling_interval')).total_seconds()

        self.config = config

        self._publisher_ready_time = publisher_ready_time
        self._publisher = None
        self.loop = True
        self._set_signal_shutdown()

        last_fetch_time = datetime.now(UTC) - fetch_back_time
        set_last_fetch(last_fetch_time)

    def _set_signal_shutdown(self):
        """Set a signal to handle shutdown."""
        signal.signal(signal.SIGTERM, self.close)

    def _start_communication(self):
        """Set up the Posttroll communication and start the publisher."""
        self._publisher = create_publisher_from_dict_config(self.config['publisher'])
        with sleeper(self._publisher_ready_time):
            self._publisher.start()

    def run(self):
        """Start the s3-stalker daemon/runner in a thread."""
        logger.info("Starting up s3stalker.")
        self._start_communication()

        while self.loop:
            with sleeper(self._wait_seconds):
                self._fetch_bucket_content_and_publish_new_files()

    def _fetch_bucket_content_and_publish_new_files(self):
        """Go through all messages in list and publish them one after the other."""
        messages = create_messages_for_recent_files(self.bucket, self.config)
        for message in messages:
            logger.info("Publishing %s", str(message))
            self._publisher.send(str(message))

    def close(self, *args, **kwargs):
        """Shutdown the S3Stalker runner."""
        logger.info('Terminating the S3 Stalker daemon/runner.')
        self.loop = False
        if self._publisher:
            self._publisher.stop()
