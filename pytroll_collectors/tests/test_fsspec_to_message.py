"""Tests for the fsspec to message features."""

from copy import deepcopy
import os
import pytest

import pytroll_collectors.fsspec_to_message
from pytroll_collectors.tests.test_s3stalker import ls_output, fs_json, subject, zip_content, zip_json, zip_json_fo


class TestMessageComposer:
    """Test case for the message composer."""

    def setup(self):
        """Set up message composer tests."""
        self.ls_output = deepcopy(ls_output)

    def test_message_is_created_with_fs(self):
        """Test the message is created with a filesystem."""
        message = pytroll_collectors.fsspec_to_message.create_message(fs_json, self.ls_output[0], subject)
        assert 'filesystem' in message.data
        assert message.data['filesystem'] == {"cls": "s3fs.core.S3FileSystem",
                                              "protocol": "s3", "args": [], "anon": True}

    def test_message_is_created_with_uri(self):
        """Test message has a uri."""
        message = pytroll_collectors.fsspec_to_message.create_message(fs_json, self.ls_output[0], subject)
        assert 'uri' in message.data
        assert message.data['uri'] == 's3://' + self.ls_output[0]['name']

    def test_message_is_created_with_uid(self):
        """Test message has a uid."""
        message = pytroll_collectors.fsspec_to_message.create_message(fs_json, self.ls_output[0], subject)
        assert 'uid' in message.data

    def test_message_is_created_with_file(self):
        """Test message has a type of file."""
        message = pytroll_collectors.fsspec_to_message.create_message(fs_json, self.ls_output[0], subject)
        assert message.type == 'file'

    def test_message_is_created_with_dataset(self):
        """Test message is created with dataset."""
        message = pytroll_collectors.fsspec_to_message.create_message(fs_json, self.ls_output, subject)
        assert 'dataset' in message.data
        assert len(message.data['dataset']) > 0
        filenames = ["s3://" + item['name'] for item in self.ls_output]
        for item in message.data['dataset']:
            assert item['uri'] in filenames
        assert message.type == 'dataset'

    def test_message_from_zip_is_created_with_uri(self):
        """Test message has a uri."""
        zip_output = list(zip_content.values())
        message = pytroll_collectors.fsspec_to_message.create_message(zip_json, zip_output[0], subject)
        assert 'uri' in message.data
        assert message.data['uri'] == 'zip://' + zip_output[0]['name']

    def test_message_from_zip_with_fo_is_created_with_uri(self):
        """Test message has a uri."""
        zip_output = list(zip_content.values())
        message = pytroll_collectors.fsspec_to_message.create_message(zip_json_fo, zip_output[0], subject)
        assert 'uri' in message.data
        assert message.data['uri'] == 'zip://' + zip_output[0]['name']

    def test_message_include_file_metadata(self):
        """Test message has metadata."""
        self.ls_output[0]['metadata'] = dict(platform_name='S3A')
        message = pytroll_collectors.fsspec_to_message.create_message(fs_json, self.ls_output[0], subject)
        assert message.data['platform_name'] == 'S3A'

    def test_message_include_metadata_parameter(self):
        """Test message has metadata from parameter."""
        metadata = dict(platform_name='S3A')
        message = pytroll_collectors.fsspec_to_message.create_message(fs_json, self.ls_output[0], subject, metadata)
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
             "uri": f"{packing}:/{filename}",
             "uid": f"{packing}:/{filename}::file://{path}"} for filename in filenames]}

        assert msg.data == expected_data
        assert msg.subject == topic
