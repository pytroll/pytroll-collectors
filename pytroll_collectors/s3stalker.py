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
"""Module to file new files on an s3 bucket."""

import datetime
import json

import fsspec.implementations.zip
import s3fs
from dateutil import tz
from posttroll.message import Message

last_fetch = datetime.datetime.now(tz.UTC) - datetime.timedelta(hours=12)


def get_last_files(path, *args, **kwargs):
    """Get the last files from path (s3 bucket and directory)."""
    fs = s3fs.S3FileSystem(*args, **kwargs)
    files = fs.ls(path, detail=True)
    files = list(filter((lambda x: x['LastModified'] > last_fetch), files))
    newest_files = sorted(files, key=(lambda x: x['LastModified']), reverse=True)
    if newest_files:
        set_last_fetch(newest_files[0]['LastModified'])
    return fs, files


def set_last_fetch(timestamp):
    """Set the last fetch time."""
    global last_fetch
    last_fetch = timestamp


def create_message(fs, file, subject):
    """Create a message to send."""
    if isinstance(file, (list, tuple)):
        metadata = {'dataset': []}
        for file_item in file:
            metadata['dataset'].append(_create_message_metadata(fs, file_item))
    else:
        metadata = _create_message_metadata(fs, file)
    return Message(subject, 'dataset', metadata)


def _create_message_metadata(fs, file):
    """Create a message to send."""
    loaded_fs = json.loads(fs)
    protocol = loaded_fs["protocol"]
    if protocol == 'abstract' and 'zip' in loaded_fs['cls']:
        protocol = 'zip'
    uri = protocol + '://' + file['name']
    uid = uri
    if 'target_protocol' in loaded_fs:
        uid += '::' + loaded_fs['target_protocol'] + '://' + loaded_fs['args'][0]
    return {'fs': loaded_fs, 'uri': uri, 'uid': uid}


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
            messages.append(create_message(zipfs.to_json(), file_list, subject))
        else:
            messages.append(create_message(fs.to_json(), file, subject))
    return messages
