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
"""Module to find new files on an s3 bucket."""

import json
import logging
import posixpath
from datetime import datetime, timedelta
import time
from contextlib import contextmanager
import fsspec.implementations.zip
import s3fs
from dateutil import tz
from posttroll.message import Message
from posttroll.publisher import Publish
from trollsift import Parser


logger = logging.getLogger(__name__)


@contextmanager
def sleeper(duration):
    """Make sure the block takes at least *duration* seconds."""
    start_time = datetime.utcnow()
    yield
    end_time = datetime.utcnow()
    waiting_time = duration - (end_time - start_time).total_seconds()
    time.sleep(max(waiting_time, 0))


class DatetimeHolder:
    """Holder for the last_fetch datetime."""

    last_fetch = datetime.now(tz.UTC) - timedelta(hours=12)


def get_last_files(path, *args, pattern=None, **kwargs):
    """Get the last files from path (s3 bucket and directory)."""
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
    files = list(filter((lambda x: x['LastModified'] > DatetimeHolder.last_fetch), files))
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


def set_last_fetch(timestamp):
    """Set the last fetch time."""
    DatetimeHolder.last_fetch = timestamp


def create_message(fs, file, subject, metadata=None):
    """Create a message to send."""
    if isinstance(file, (list, tuple)):
        file_data = {'dataset': []}
        for file_item in file:
            file_data['dataset'].append(_create_message_metadata(fs, file_item))
        message_type = 'dataset'
    else:
        file_data = _create_message_metadata(fs, file)
        message_type = 'file'
    if metadata:
        file_data.update(metadata)
    return Message(subject, message_type, file_data)


def _create_message_metadata(fs, file):
    """Create a message to send."""
    loaded_fs = json.loads(fs)
    uri = _create_uri(file, loaded_fs)
    uid = _create_uid_from_uri(uri, loaded_fs)
    base_data = {'filesystem': loaded_fs, 'uri': uri, 'uid': uid}
    base_data.update(file.get('metadata', dict()))
    return base_data


def _create_uri(file, loaded_fs):
    protocol = loaded_fs["protocol"]
    if protocol == 'abstract' and 'zip' in loaded_fs['cls']:
        protocol = 'zip'
    uri = protocol + ':///' + file['name']
    return uri


def _create_uid_from_uri(uri, loaded_fs):
    uid = uri
    if 'target_protocol' in loaded_fs:
        uid += '::' + loaded_fs['target_protocol'] + ':///' + (loaded_fs.get('fo') or loaded_fs['args'][0])
    return uid


def filelist_to_messages(fs, files, subject):
    """Convert filelist to a list of posttroll messages."""
    return [create_message(fs.to_json(), file, subject) for file in files]


def filelist_unzip_to_messages(fs, files, subject):
    """Unzip files in filelist if necessary, create posttroll messages."""
    messages = []
    for file in files:
        if file['name'].endswith('.zip'):
            zipfs = fsspec.implementations.zip.ZipFileSystem(fo=file['name'],
                                                             target_protocol=fs.protocol[0],
                                                             target_options=fs.storage_options)
            file_list = list(zipfs.find('/', detail=True).values())
            messages.append(create_message(zipfs.to_json(), file_list, subject, file.get('metadata')))
        else:
            messages.append(create_message(fs.to_json(), file, subject))
    return messages


def publish_new_files(bucket, config):
    """Publish files newly arrived in bucket."""
    with Publish("s3_stalker") as pub:
        time_back = config['timedelta']
        subject = config['subject']
        pattern = config.get('file_pattern')
        with sleeper(2.5):
            set_last_fetch(datetime.now(tz.UTC) - timedelta(**time_back))
            s3_kwargs = config['s3_kwargs']
            fs, files = get_last_files(bucket, pattern=pattern, **s3_kwargs)
            messages = filelist_unzip_to_messages(fs, files, subject)

        for message in messages:
            logger.info("Publishing %s", str(message))
            pub.send(str(message))
