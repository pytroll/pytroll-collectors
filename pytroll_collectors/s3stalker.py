#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
"""Module to find new files on an s3 bucket.

The contents of the yaml configuration file should look like this::

    s3_kwargs:
      anon: false
      client_kwargs:
        aws_access_key_id: my_accesskey
        aws_secret_access_key: my_secret_key
        endpoint_url: https://xxx.yyy.zz
    fetch_back_to:
      hours: 20
    file_pattern: '{platform_name:3s}_OL_2_{datatype_id:_<6s}_{start_time:%Y%m%dT%H%M%S}_{end_time:%Y%m%dT%H%M%S}_{creation_time:%Y%m%dT%H%M%S}_{duration:4d}_{cycle:3d}_{relative_orbit:3d}_{frame:4d}_{centre:3s}_{mode:1s}_{timeliness:2s}_{collection:3s}.zip'
    subject: /segment/2/safe-olci/S3/
"""  # noqa

import logging
import posixpath
from datetime import datetime, timedelta
import time
from dateutil import tz
from contextlib import contextmanager
import s3fs
from posttroll.publisher import Publish
from trollsift import Parser

from pytroll_collectors.fsspec_to_message import filelist_unzip_to_messages

logger = logging.getLogger(__name__)


@contextmanager
def sleeper(duration):
    """Make sure the block takes at least *duration* seconds."""
    start_time = datetime.utcnow()
    yield
    end_time = datetime.utcnow()
    waiting_time = duration - (end_time - start_time).total_seconds()
    logger.debug('waiting time: %f', waiting_time)
    time.sleep(max(waiting_time, 0))


class DatetimeHolder:
    """Holder for the last_fetch datetime."""

    last_fetch = datetime.now(tz.UTC) - timedelta(hours=12)


def set_last_fetch(timestamp):
    """Set the last fetch time."""
    DatetimeHolder.last_fetch = timestamp


def get_last_fetch():
    """Get the last fetch time."""
    return DatetimeHolder.last_fetch


def get_last_files(path, *args, pattern=None, **kwargs):
    """Get the last files from path (s3 bucket and directory)."""
    kwargs['skip_instance_cache'] = True
    fs = s3fs.S3FileSystem(*args, **kwargs)
    files = _get_files_since_last_fetch(fs, path)
    files = _match_files_to_pattern(files, path, pattern)
    _reset_last_fetch_from_file_list(files)
    return fs, files


def _reset_last_fetch_from_file_list(files):
    newest_files = sorted(list(files), key=(lambda x: x['LastModified']), reverse=True)
    if newest_files:
        set_last_fetch(newest_files[0]['LastModified'])


def _get_files_since_last_fetch(fs, path):
    files = fs.ls(path, detail=True)
    logger.debug(f"Get files since {get_last_fetch()}")
    files = list(filter((lambda x: x['LastModified'] > get_last_fetch()), files))
    return files


def _match_files_to_pattern(files, path, pattern):
    if pattern is not None:
        parser = Parser(posixpath.join(path, pattern))
        matching_files = []
        for file in files:
            try:
                metadata = parser.parse(file['name'])
                file['metadata'] = metadata
                matching_files.append(file)
            except ValueError:
                pass
        return matching_files
    return files


def publish_new_files(bucket, config, publisher_ready_time=2.5):
    """Publish files newly arrived in bucket."""
    time_back = config.pop('fetch_back_to')
    set_last_fetch(datetime.now(tz.UTC) - timedelta(**time_back))
    with Publish("s3_stalker") as pub:
        with sleeper(publisher_ready_time):
            messages = create_messages_for_recent_files(bucket, config)
        for message in messages:
            logger.info("Publishing %s", str(message))
            pub.send(str(message))


def create_messages_for_recent_files(bucket, config):
    """Create messages for recent files and return."""
    logger.debug("Create messages for recent files...")

    logger.debug(f"Last fetch is {get_last_fetch()}")
    subject = config['subject']
    pattern = config.get('file_pattern')
    s3_kwargs = config['s3_kwargs']
    fs_, files = get_last_files(bucket, pattern=pattern, **s3_kwargs)
    messages = filelist_unzip_to_messages(fs_, files, subject)
    return messages
