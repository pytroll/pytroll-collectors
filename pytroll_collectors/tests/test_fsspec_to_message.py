"""Tests for the fsspec to message features."""
import json
import os
import socket
from copy import deepcopy

import pytest
import pytroll_collectors.fsspec_to_message
from pytroll_collectors.fsspec_to_message import extract_local_files_to_message_for_remote_use
from pytroll_collectors.tests.test_s3stalker import ls_output, fs_json, subject, zip_content, zip_json, zip_json_fo


class TestMessageComposer:
    """Test case for the message composer."""

    def setup_method(self):
        """Set up message composer tests."""
        self.ls_output = deepcopy(ls_output)

    def test_message_is_created_with_fs(self):
        """Test the message is created with a filesystem."""
        message = pytroll_collectors.fsspec_to_message.create_message_with_json_fs(fs_json, self.ls_output[0], subject)
        assert 'filesystem' in message.data
        assert message.data['filesystem'] == {"cls": "s3fs.core.S3FileSystem",
                                              "protocol": "s3", "args": [], "anon": True}

    def test_message_is_created_with_uri(self):
        """Test message has a uri."""
        message = pytroll_collectors.fsspec_to_message.create_message_with_json_fs(fs_json, self.ls_output[0], subject)
        assert 'uri' in message.data
        assert message.data['uri'] == 's3://' + self.ls_output[0]['name']

    def test_message_is_created_with_uid(self):
        """Test message has a uid."""
        message = pytroll_collectors.fsspec_to_message.create_message_with_json_fs(fs_json, self.ls_output[0], subject)
        assert 'uid' in message.data

    def test_message_is_created_with_file(self):
        """Test message has a type of file."""
        message = pytroll_collectors.fsspec_to_message.create_message_with_json_fs(fs_json, self.ls_output[0], subject)
        assert message.type == 'file'

    def test_message_is_created_with_dataset(self):
        """Test message is created with dataset."""
        message = pytroll_collectors.fsspec_to_message.create_message_with_json_fs(fs_json, self.ls_output, subject)
        assert 'dataset' in message.data
        assert len(message.data['dataset']) > 0
        filenames = ["s3://" + item['name'] for item in self.ls_output]
        for item in message.data['dataset']:
            assert item['uri'] in filenames
        assert message.type == 'dataset'

    def test_message_from_zip_is_created_with_uid(self):
        """Test message has a uri."""
        zip_output = list(zip_content.values())
        message = pytroll_collectors.fsspec_to_message.create_message_with_json_fs(zip_json, zip_output[0], subject)
        assert 'uid' in message.data
        assert message.data['uid'] == 'zip://' + zip_output[0]['name']

    def test_message_from_zip_with_fo_is_created_with_uid(self):
        """Test message has a uri."""
        zip_output = list(zip_content.values())
        message = pytroll_collectors.fsspec_to_message.create_message_with_json_fs(zip_json_fo, zip_output[0], subject)
        assert 'uid' in message.data
        assert message.data['uid'] == 'zip://' + zip_output[0]['name']

    def test_message_include_file_metadata(self):
        """Test message has metadata."""
        self.ls_output[0]['metadata'] = dict(platform_name='S3A')
        message = pytroll_collectors.fsspec_to_message.create_message_with_json_fs(fs_json, self.ls_output[0], subject)
        assert message.data['platform_name'] == 'S3A'

    def test_message_include_metadata_parameter(self):
        """Test message has metadata from parameter."""
        metadata = dict(platform_name='S3A')
        message = pytroll_collectors.fsspec_to_message.create_message_with_json_fs(fs_json,
                                                                                   self.ls_output[0],
                                                                                   subject,
                                                                                   metadata)
        assert message.data['platform_name'] == 'S3A'


def create_tar_file(filenames, path):
    """Create a tarred file."""
    from tarfile import TarFile

    with TarFile(path, mode="w") as packfile:
        for filename in filenames:
            packfile.add(filename)


def create_zip_file(filenames, path):
    """Create a zipped file."""
    from zipfile import ZipFile

    with ZipFile(path, mode="w") as packfile:
        for filename in filenames:
            packfile.write(filename)


def create_pack_path(tmp_path):
    """Create the pack path."""
    pack_path = tmp_path / "pack"
    os.mkdir(pack_path)
    return pack_path


class TestUnpackMessage:
    """Test unpacking a file to message."""

    def create_files_to_pack(self, tmp_path):
        """Create files to pack."""
        filenames = []
        for filename_number in range(10):
            filename = tmp_path / f"file_to_pack{filename_number}.txt"
            with open(filename, mode="w") as fd:
                fd.write("Very important stuff.\n")
            filenames.append(filename)
        return filenames

    @pytest.mark.parametrize(
        ("packing", "create_packfile", "filesystem_class"),
        [
            ("tar", create_tar_file, "fsspec.implementations.tar.TarFileSystem"),
            ("zip", create_zip_file, "fsspec.implementations.zip.ZipFileSystem"),
        ]
    )
    def test_pack_file_extract(self, packing, create_packfile, filesystem_class, tmp_path):
        """Test extracting packed files."""
        filenames = self.create_files_to_pack(tmp_path)

        pack_path = create_pack_path(tmp_path)
        path = pack_path / ("packfile." + packing)
        create_packfile(filenames, path)

        from pytroll_collectors.fsspec_to_message import extract_files_to_message
        import fsspec
        fs = fsspec.filesystem("file")
        packed_file = path
        topic = "interesting_topic"
        msg = extract_files_to_message(packed_file, fs, topic, packing)
        expected_data = {"dataset": [
            {"filesystem": {"cls": filesystem_class,
                            "protocol": packing, "args": [],
                            "fo": f"{path}",
                            "target_protocol": "file", "target_options": {}},
             "uid": f"{packing}:/{filename}",
             "uri": f"{packing}:/{filename}::file://{path}"} for filename in filenames]}

        assert msg.data == expected_data
        assert msg.subject == topic

    @pytest.mark.parametrize(
        ("packing", "create_packfile", "filesystem_class"),
        [
            ("tar", create_tar_file, "fsspec.implementations.tar.TarFileSystem"),
            ("zip", create_zip_file, "fsspec.implementations.zip.ZipFileSystem"),
        ]
    )
    def test_pack_local_file_extract(self, packing, create_packfile, filesystem_class, tmp_path):
        """Test extracting packed files."""
        filenames = self.create_files_to_pack(tmp_path)

        pack_path = create_pack_path(tmp_path)
        path = pack_path / ("packfile." + packing)
        create_packfile(filenames, path)

        packed_file = path
        topic = "interesting_topic"
        msg = extract_local_files_to_message_for_remote_use(packed_file, topic, packing=packing)
        host = socket.gethostname()
        expected_data = {"dataset": [
            {"filesystem": {"cls": filesystem_class,
                            "protocol": packing, "args": [],
                            "fo": f"{path}",
                            "target_protocol": "ssh", "target_options": {"host": host}},
             "uid": f"{packing}:/{filename}",
             "uri": f"{packing}:/{filename}::ssh://{host}{path}"} for filename in filenames]}

        assert msg.data == expected_data
        assert msg.subject == topic

    @pytest.mark.skipif(os.getenv("GITHUB_ACTIONS", "false") == "true", reason="SSH filesystem shaky on github actions")
    @pytest.mark.parametrize(
        ("packing", "create_packfile", "filesystem_class"),
        [
            ("tar", create_tar_file, "fsspec.implementations.tar.TarFileSystem"),
            ("zip", create_zip_file, "fsspec.implementations.zip.ZipFileSystem"),
        ]
    )
    def test_pack_local_file_extract_filesystem(self, packing, create_packfile, filesystem_class, tmp_path):
        """Test that extracting packed files give a valid filesystem."""
        filenames = self.create_files_to_pack(tmp_path)

        pack_path = create_pack_path(tmp_path)
        path = pack_path / ("packfile." + packing)
        create_packfile(filenames, path)

        packed_file = path
        topic = "interesting_topic"
        msg = extract_local_files_to_message_for_remote_use(packed_file, topic, packing=packing)

        filesystem_info = json.dumps(msg.data["dataset"][0]["filesystem"])
        self.check_filesystem_is_understood_by_fsspec(filesystem_info)

    def check_filesystem_is_understood_by_fsspec(self, filesystem_info):
        """Check that the filesystem info is understood by fsspec."""
        from fsspec import AbstractFileSystem
        return AbstractFileSystem.from_json(filesystem_info)

    @pytest.mark.parametrize(
        ("packing", "create_packfile", "filesystem_class"),
        [
            ("tar", create_tar_file, "fsspec.implementations.tar.TarFileSystem"),
            ("zip", create_zip_file, "fsspec.implementations.zip.ZipFileSystem"),
        ]
    )
    def test_pack_local_file_extract_with_custom_options(self, packing, create_packfile, filesystem_class, tmp_path):
        """Test extracting packed files using custom options."""
        filenames = self.create_files_to_pack(tmp_path)

        pack_path = create_pack_path(tmp_path)
        path = pack_path / ("packfile." + packing)
        create_packfile(filenames, path)

        packed_file = path
        topic = "interesting_topic"
        host = socket.gethostname()
        username = "me"
        protocol = "ssh"
        port = 22
        target_options = {"host": host,
                          "username": username,
                          "protocol": protocol,
                          "port": port}
        msg = extract_local_files_to_message_for_remote_use(packed_file, topic,
                                                            target_options=target_options, packing=packing)
        expected_data = {"dataset": [
            {"filesystem": {"cls": filesystem_class,
                            "protocol": packing, "args": [],
                            "fo": f"{path}",
                            "target_protocol": protocol, "target_options": target_options},
             "uid": f"{packing}:/{filename}",
             "uri": f"{packing}:/{filename}::{protocol}://{username}@{host}:{port}{path}"}
            for filename in filenames]}

        assert msg.data == expected_data
        assert msg.subject == topic


class TestSingleFile:
    """Test creating messages from a single file."""

    def test_local_file_to_remote_message(self, tmp_path):
        """Test creating a message for a single file with custom options."""
        filename = tmp_path / "important_file"
        with open(filename, mode="w") as fd:
            fd.write("Very important stuff.\n")

        topic = "interesting_topic"
        host = socket.gethostname()
        username = "me"
        protocol = "ssh"
        port = 22
        target_options = {"host": host,
                          "username": username,
                          "protocol": protocol,
                          "port": port}
        msg = extract_local_files_to_message_for_remote_use(filename, topic,
                                                            target_options=target_options)
        filesystem_dict = {"cls": "fsspec.implementations.sftp.SFTPFileSystem",
                           "protocol": "ssh", "args": []}
        filesystem_dict.update(target_options)
        expected_data = {
            "filesystem": filesystem_dict,
            "uid": f"{protocol}://{username}@{host}:{port}{filename}",
            "uri": f"{protocol}://{username}@{host}:{port}{filename}"}

        assert msg.data == expected_data
        assert msg.subject == topic

    def test_remote_message_cannot_contain_password(self, tmp_path):
        """Test creating a message with a password crashes."""
        filename = tmp_path / "important_file"
        with open(filename, mode="w") as fd:
            fd.write("Very important stuff.\n")

        topic = "interesting_topic"
        host = socket.gethostname()
        username = "me"
        password = "the_password"
        protocol = "ssh"
        port = 22
        target_options = {"host": host,
                          "username": username,
                          "password": password,
                          "protocol": protocol,
                          "port": port}

        with pytest.raises(RuntimeError):
            _ = extract_local_files_to_message_for_remote_use(filename, topic,
                                                              target_options=target_options)


def test_message_has_no_filesystem():
    """Test that 'secret' and 'key' are stripped from the message."""
    import datetime as dt
    from dateutil import tz
    from pytroll_collectors.fsspec_to_message import create_message_with_json_fs

    jsn = ('{"cls": "s3fs.core.S3FileSystem", "protocol": "s3", "args": [], '
           '"client_kwargs": {"endpoint_url": "https://lake.fmi.fi", '
           '"aws_access_key_id": "xxx", "aws_secret_access_key": "yyy"}, '
           '"secret": "xxx", "key": "yyy", "anon": false}')
    file_ = {'key': 'bucket/test.bin', 'LastModified': dt.datetime.now(tz.UTC),
             'name': 'bucket/test.bin', 'metadata': {}}
    msg = create_message_with_json_fs(jsn, file_, '/subject')

    assert 'secret' not in msg.data['filesystem']
    assert 'key' not in msg.data['filesystem']
    assert 'aws_access_key_id' not in msg.data['filesystem']['client_kwargs']
    assert 'aws_secret_access_key' not in msg.data['filesystem']['client_kwargs']
