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
"""Test s3stalker daemon."""
import datetime
from copy import deepcopy
from unittest import mock
import time

from dateutil.tz import tzutc, UTC
from freezegun import freeze_time

from pytroll_collectors.s3stalker import set_last_fetch,  _match_files_to_pattern, create_messages_for_recent_files
from pytroll_collectors.s3stalker_daemon_runner import S3StalkerRunner
from pytroll_collectors.tests.test_s3stalker import fs_json, FakePublisher

S3_STALKER_CONFIG = {'s3_kwargs': {'anon': False, 'client_kwargs': {'endpoint_url': 'https://xxx.yyy.zz',
                                                                    'aws_access_key_id': 'my_accesskey',
                                                                    'aws_secret_access_key': 'my_secret_key'}},
                     'fetch_back_to': {'hours': 1},
                     'polling_interval': {'minutes': 2},
                     'subject': '/yuhu',
                     'file_pattern': '{channel:5s}_{platform_name:3s}_d{start_time:%Y%m%d_t%H%M%S}{frac:1s}_e{end_time:%H%M%S}{frac_end:1s}_b{orbit_number:5s}_c{process_time:20s}_cspp_dev.h5',  # noqa
                     'publisher': {'name': 's3stalker_runner'}}

ATMS_FILES = [{'Key': 'atms-sdr/GATMO_j01_d20221220_t1230560_e1231276_b26363_c20221220124753607778_cspp_dev.h5',
               'LastModified': datetime.datetime(2022, 12, 20, 12, 48, 25, 173000, tzinfo=tzutc()),
               'ETag': '"bb037828c47d28a30ce6d49e719b6c64"',
               'Size': 155964,
               'StorageClass': 'STANDARD',
               'type': 'file',
               'size': 155964,
               'name': 'atms-sdr/GATMO_j01_d20221220_t1230560_e1231276_b26363_c20221220124753607778_cspp_dev.h5'},
              {'Key': 'atms-sdr/GATMO_j01_d20221220_t1231280_e1231596_b26363_c20221220124754976465_cspp_dev.h5',
               'LastModified': datetime.datetime(2022, 12, 20, 12, 48, 25, 834000, tzinfo=tzutc()),
               'ETag': '"327b7e1300700f55268cc1f4dc133459"',
               'Size': 156172,
               'StorageClass': 'STANDARD',
               'type': 'file',
               'size': 156172,
               'name': 'atms-sdr/GATMO_j01_d20221220_t1231280_e1231596_b26363_c20221220124754976465_cspp_dev.h5'},
              {'Key': 'atms-sdr/SATMS_npp_d20221220_t1330400_e1331116_b57761_c20221220133901538622_cspp_dev.h5',
               'LastModified': datetime.datetime(2022, 12, 20, 13, 39, 33, 86000, tzinfo=tzutc()),
               'ETag': '"2fe59174e29627acd82a28716b18d92a"',
               'Size': 168096,
               'StorageClass': 'STANDARD',
               'type': 'file',
               'size': 168096,
               'name': 'atms-sdr/SATMS_npp_d20221220_t1330400_e1331116_b57761_c20221220133901538622_cspp_dev.h5'},
              {'Key': 'atms-sdr/SATMS_npp_d20221220_t1331120_e1331436_b57761_c20221220133902730925_cspp_dev.h5',
               'LastModified': datetime.datetime(2022, 12, 20, 13, 39, 33, 798000, tzinfo=tzutc()),
               'ETag': '"ffff983cdf767ab635a7ae51dc7d0626"',
               'Size': 167928,
               'StorageClass': 'STANDARD',
               'type': 'file',
               'size': 167928,
               'name': 'atms-sdr/SATMS_npp_d20221220_t1331120_e1331436_b57761_c20221220133902730925_cspp_dev.h5'}]


def test_match_files_to_pattern():
    """Test matching files to pattern."""
    path = 'atms-sdr'
    pattern = 'GATMO_{platform_name:3s}_d{start_time:%Y%m%d_t%H%M%S}{frac:1s}_e{end_time:%H%M%S}{frac_end:1s}_b{orbit_number:5s}_c{process_time:20s}_cspp_dev.h5'  # noqa

    res_files = _match_files_to_pattern(ATMS_FILES, path, pattern)

    assert res_files == ATMS_FILES[0:2]


class TestS3StalkerRunner:
    """Test the S3 Stalker Runner functionalities."""

    def setup_method(self):
        """Set up the test case."""
        self.ls_output = deepcopy(ATMS_FILES)
        start_time = datetime.datetime(2022, 12, 20, 12, 0)
        now = datetime.datetime.utcnow()
        self.delta_sec = (now - start_time).total_seconds()
        self.bucket = 'atms-sdr'
        self.config = S3_STALKER_CONFIG

    @mock.patch('s3fs.S3FileSystem')
    def test_create_messages_for_recent_files(self, s3_fs):
        """Test create messages for recent files arriving in the bucket."""
        s3_fs.return_value.ls.return_value = self.ls_output
        s3_fs.return_value.to_json.return_value = fs_json

        s3runner = S3StalkerRunner(self.bucket, S3_STALKER_CONFIG.copy())

        s3runner._timedelta = {'seconds': self.delta_sec}
        last_fetch_time = datetime.datetime.now(UTC) - datetime.timedelta(**s3runner._timedelta)
        set_last_fetch(last_fetch_time)
        result_msgs = create_messages_for_recent_files(s3runner.bucket, s3runner.config)

        assert len(result_msgs) == 4
        msg = result_msgs[0]
        assert msg.data['orbit_number'] == '26363'
        assert msg.data['platform_name'] == 'j01'
        assert msg.data['frac_end'] == '6'
        assert msg.data['start_time'] == datetime.datetime(2022, 12, 20, 12, 30, 56)
        assert msg.data['end_time'] == datetime.datetime(1900, 1, 1, 12, 31, 27)
        assert msg.data["process_time"] == "20221220124753607778"

    @mock.patch('s3fs.S3FileSystem')
    @mock.patch('pytroll_collectors.s3stalker_daemon_runner.create_publisher_from_dict_config')
    def test_fetch_new_files_publishes_messages(self, create_publisher, s3_fs):
        """Test process the messages."""
        s3_fs.return_value.ls.return_value = self.ls_output[:2]
        s3_fs.return_value.to_json.return_value = fs_json

        publisher = FakePublisher("fake_publisher")
        create_publisher.return_value = publisher
        before_files_arrived = datetime.datetime(2022, 12, 20, 12, 0, 0, tzinfo=UTC)

        stalker_config = S3_STALKER_CONFIG.copy()
        stalker_config["polling_interval"] = dict(seconds=.2)

        s3runner = None
        try:

            with freeze_time(before_files_arrived + datetime.timedelta(hours=1)):
                s3runner = S3StalkerRunner(self.bucket, stalker_config, 0)
                s3runner.start()
                time.sleep(.1)
                assert len(publisher.messages_sent) == 2
                first_messages_sent = publisher.messages_sent
                publisher.clear_sent_messages()

            with freeze_time(before_files_arrived + datetime.timedelta(hours=2)):
                s3_fs.return_value.ls.return_value = self.ls_output
                time.sleep(.1)
                assert len(publisher.messages_sent) == 2
                assert publisher.messages_sent != first_messages_sent
        finally:
            if s3runner:
                s3runner.close()
