"""Function to create posttroll message containing fsspec filesystem specifications."""

import json
import os
import socket

from fsspec import get_filesystem_class
from posttroll.message import Message


def create_message_with_json_fs(fs_to_json, file, subject, metadata=None):
    """Create a message to send."""
    if isinstance(file, (list, tuple)):
        file_data = {'dataset': []}
        for file_item in file:
            file_data['dataset'].append(_create_message_metadata(fs_to_json, file_item))
        message_type = 'dataset'
    else:
        file_data = _create_message_metadata(fs_to_json, file)
        message_type = 'file'
    if metadata:
        file_data.update(metadata)
    return Message(subject, message_type, file_data)


def _create_message_metadata(fs, file):
    """Create a message to send."""
    loaded_fs = json.loads(fs)
    loaded_fs.pop('key', None)
    loaded_fs.pop('secret', None)
    if 'client_kwargs' in loaded_fs:
        loaded_fs['client_kwargs'].pop('aws_access_key_id', None)
        loaded_fs['client_kwargs'].pop('aws_secret_access_key', None)
    uid = _create_uid(file, loaded_fs)
    uri = _create_uri_from_uid(uid, loaded_fs)
    base_data = {'filesystem': loaded_fs, 'uri': uri, 'uid': uid}
    base_data.update(file.get('metadata', dict()))
    return base_data


def _create_uid(file, loaded_fs):
    protocol = loaded_fs["protocol"]
    if protocol == 'abstract' and 'zip' in loaded_fs['cls']:
        protocol = 'zip'
    netloc = create_netloc(loaded_fs)
    uid = protocol + '://' + netloc + file['name']
    return uid


def _create_uri_from_uid(uid, loaded_fs):
    uri = uid
    if 'target_protocol' in loaded_fs:
        netloc = create_netloc(loaded_fs['target_options'])
        uri += '::' + loaded_fs['target_protocol'] + '://' + netloc + (loaded_fs.get('fo') or loaded_fs['args'][0])
    return uri


def create_netloc(ssh):
    """Create the netloc from ssh parameters like `host`, `port`, `username`, `password`."""
    host = ssh.get("host")
    if host is None:
        return ""
    username = ssh.get("username")
    password = ssh.get("password")
    if password is not None:
        raise RuntimeError("Do not want to send password in clear text.")
    port = ssh.get("port")
    userpass = (
        username + ((":" + password) if password is not None else "") + "@"
        if username is not None
        else ""
    )
    netloc = host + ((":" + str(port)) if port is not None else "")
    return userpass + netloc


def filelist_to_messages(fs, files, subject):
    """Convert filelist to a list of posttroll messages."""
    return [create_message_with_json_fs(fs.to_json(), file, subject) for file in files]


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
        return create_message_with_json_fs(fs.to_json(), file, subject)

    fs_class = get_filesystem_class(packing)

    protocol = _get_fs_protocol(fs)
    packfs = fs_class(fo=filename,
                      target_protocol=protocol,
                      target_options=fs.storage_options)
    file_list = list(packfs.find('/', detail=True).values())
    return create_message_with_json_fs(packfs.to_json(), file_list, subject, file.get('metadata'))


def extract_local_files_to_message_for_remote_use(filename, subject, packing=None,
                                                  target_protocol=None, target_options=None):
    """Try extracting a file virtually and create the corresponding message.

    If the file is not an archive, create a message with the original file instead.
    """
    if packing is None:
        cls = get_filesystem_class(target_protocol or "ssh")
        cls = ".".join((cls.__module__, cls.__name__))
        target_options = target_options or dict(host=socket.gethostname())
        target_options.setdefault("protocol", target_protocol or "ssh")
        fs_dict = dict(cls=cls,
                       args=[],
                       **target_options
                       )
        file = dict(name=os.fspath(filename))
    else:
        fs_class = get_filesystem_class(packing)
        packfs = fs_class(fo=os.fspath(filename))
        file = list(packfs.find('/', detail=True).values())
        fs_dict = json.loads(packfs.to_json())
        fs_dict["target_protocol"] = target_protocol or "ssh"
        fs_dict["target_options"] = target_options or {"host": socket.gethostname()}

    return create_message_with_json_fs(json.dumps(fs_dict), file, subject)


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
