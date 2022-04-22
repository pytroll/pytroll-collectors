"""Function to create posttroll message containing fsspec filesystem specifications."""

import json
import os

from fsspec import get_filesystem_class
from posttroll.message import Message


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
    uri = protocol + '://' + file['name']
    return uri


def _create_uid_from_uri(uri, loaded_fs):
    uid = uri
    if 'target_protocol' in loaded_fs:
        uid += '::' + loaded_fs['target_protocol'] + '://' + (loaded_fs.get('fo') or loaded_fs['args'][0])
    return uid


def filelist_to_messages(fs, files, subject):
    """Convert filelist to a list of posttroll messages."""
    return [create_message(fs.to_json(), file, subject) for file in files]


def filelist_unzip_to_messages(fs, files, subject):
    """Unzip files in filelist if necessary, create posttroll messages."""
    messages = []
    for file in files:
        packing = None
        file, filename = _get_filename(file, fs)
        if filename.endswith('.zip'):
            packing = "zip"
        elif filename.endswith('.tar'):
            packing = "tar"
        messages.append(extract_files_to_message(file, fs, subject, packing))

    return messages


def extract_files_to_message(file, fs, subject, packing=None):
    """Try extracting a file virtually and create the corresponding message.

    If the file is not an archive, create a message with the original file instead.
    """
    file, filename = _get_filename(file, fs)

    if packing is None:
        return create_message(fs.to_json(), file, subject)

    fs_class = get_filesystem_class(packing)

    protocol = _get_fs_protocol(fs)
    packfs = fs_class(fo=filename,
                      target_protocol=protocol,
                      target_options=fs.storage_options)
    file_list = list(packfs.find('/', detail=True).values())
    return create_message(packfs.to_json(), file_list, subject, file.get('metadata'))


def _get_filename(file, fs):
    try:
        filename = file["name"]
    except TypeError:
        filename = os.fspath(file)
        file = fs.info(file)
    return file, filename


def _get_fs_protocol(fs):
    if isinstance(fs.protocol, (list, tuple)):
        protocol = fs.protocol[0]
    else:
        protocol = fs.protocol
    return protocol
