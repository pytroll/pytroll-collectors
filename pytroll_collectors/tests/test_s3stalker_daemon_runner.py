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
import logging
from copy import deepcopy
from unittest import mock

from dateutil.tz import tzutc, UTC
from freezegun import freeze_time

import pytroll_collectors.s3stalker_daemon_runner
from pytroll_collectors.s3stalker import set_last_fetch, get_last_fetch, _match_files_to_pattern, \
    create_messages_for_recent_files
from pytroll_collectors.s3stalker_daemon_runner import S3StalkerRunner
from pytroll_collectors.tests.test_s3stalker import fs_json

S3_STALKER_CONFIG = {'s3_kwargs': {'anon': False, 'client_kwargs': {'endpoint_url': 'https://xxx.yyy.zz',
                                                                    'aws_access_key_id': 'my_accesskey',
                                                                    'aws_secret_access_key': 'my_secret_key'}},
                     'timedelta': {'minutes': 2},
                     'subject': '/yuhu',
                     'file_pattern': 'GATMO_{platform_name:3s}_d{start_time:%Y%m%d_t%H%M%S}{frac:1s}_e{end_time:%H%M%S}{frac_end:1s}_b{orbit_number:5s}_c{process_time:20s}_cspp_dev.h5',  # noqa
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


def test_s3stalker_runner_initialization():
    """Test initialize/instanciate the S3StalkerRunner class."""
    startup_timedelta_seconds = 1800
    bucket = 'atms-sdr'

    s3runner = S3StalkerRunner(bucket, S3_STALKER_CONFIG, startup_timedelta_seconds)

    assert s3runner.bucket == 'atms-sdr'
    assert s3runner.config == S3_STALKER_CONFIG
    assert s3runner.startup_timedelta_seconds == startup_timedelta_seconds
    assert s3runner.time_back == {'minutes': 2}
    assert s3runner._timedelta == {'minutes': 2}
    assert s3runner._wait_seconds == 120.0
    assert s3runner.publisher is None
    assert s3runner.loop is False


def test_s3stalker_runner_get_timedelta():
    """Test getting the delta time defining the how far back in time to search for new files in the bucket."""
    startup_timedelta_seconds = 2000
    bucket = 'atms-sdr'

    s3runner = S3StalkerRunner(bucket, S3_STALKER_CONFIG, startup_timedelta_seconds)

    last_fetch_time = None
    first_run = True
    result = s3runner._get_timedelta(last_fetch_time, is_first_run=first_run)

    assert result == {'seconds': 2000}


@mock.patch.object(pytroll_collectors.s3stalker_daemon_runner.S3StalkerRunner, '_process_messages')
@freeze_time('2022-12-20 10:10:0')
def test_do_fetch_most_recent(process_messages):
    """Test getting the time of the last files fetch."""
    process_messages.return_value = None

    startup_timedelta_seconds = 3600
    bucket = 'atms-sdr'

    s3runner = S3StalkerRunner(bucket, S3_STALKER_CONFIG, startup_timedelta_seconds)

    set_last_fetch(datetime.datetime.now(UTC) - datetime.timedelta(seconds=startup_timedelta_seconds))

    first_run = False
    last_fetch_time = get_last_fetch()
    s3runner._set_timedelta(last_fetch_time, first_run)
    result = get_last_fetch()

    assert result.strftime('%Y%m%d-%H%M') == '20221220-0910'


def test_match_files_to_pattern():
    """Test matching files to pattern."""
    path = 'atms-sdr'
    pattern = 'GATMO_{platform_name:3s}_d{start_time:%Y%m%d_t%H%M%S}{frac:1s}_e{end_time:%H%M%S}{frac_end:1s}_b{orbit_number:5s}_c{process_time:20s}_cspp_dev.h5'  # noqa

    res_files = _match_files_to_pattern(ATMS_FILES, path, pattern)

    assert res_files == ATMS_FILES[0:2]


class FakePublish:
    """A fake publish class with a dummy send method."""

    def __init__(self, _dummy):
        """Initialize the fake publisher class."""
        self.messages_sent = []

    def send(self, msg):
        """Faking the sending of a message."""
        self.messages_sent.append(msg)
        return msg

    def __call__(self, msg):
        """Faking a call method."""
        return self.send(msg)

    def clear_sent_messages(self):
        """Clear the sent messages."""
        self.messages_sent = []

    def start(self):
        """Start the publisher."""

    def stop(self):
        """Stop the publisher."""


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

        startup_timedelta_seconds = 2000

        s3runner = S3StalkerRunner(self.bucket, S3_STALKER_CONFIG, startup_timedelta_seconds)

        s3runner._timedelta = {'seconds': self.delta_sec}

        result_msgs = create_messages_for_recent_files(s3runner.bucket, s3runner.config, s3runner._timedelta)

        assert len(result_msgs) == 2
        msg = result_msgs[0]
        assert msg.data['orbit_number'] == '26363'
        assert msg.data['platform_name'] == 'j01'
        assert msg.data['frac_end'] == '6'
        assert msg.data['start_time'] == datetime.datetime(2022, 12, 20, 12, 30, 56)
        assert msg.data['end_time'] == datetime.datetime(1900, 1, 1, 12, 31, 27)
        assert msg.data["process_time"] == "20221220124753607778"

    @freeze_time('2022-12-20 10:10:00')
    def test_get_seconds_back_to_search(self):
        """Test get seconds back in time to search for new files."""
        startup_timedelta_seconds = 2000

        s3runner = S3StalkerRunner(self.bucket, S3_STALKER_CONFIG, startup_timedelta_seconds)

        s3runner._timedelta = {'seconds': self.delta_sec}

        result = s3runner._get_seconds_back_to_search(None)
        assert result == 120.0

        set_last_fetch(datetime.datetime.now(UTC) - datetime.timedelta(seconds=startup_timedelta_seconds))
        last_fetch_time = get_last_fetch()
        result = s3runner._get_seconds_back_to_search(last_fetch_time)
        assert result == 2000.0

    @mock.patch('s3fs.S3FileSystem')
    def test_process_messages(self, s3_fs, caplog):
        """Test process the messages."""
        s3_fs.return_value.ls.return_value = self.ls_output
        s3_fs.return_value.to_json.return_value = fs_json

        startup_timedelta_seconds = 2000

        s3runner = S3StalkerRunner(self.bucket, S3_STALKER_CONFIG, startup_timedelta_seconds)

        s3runner.publisher = FakePublish('fake_publisher')

        with caplog.at_level(logging.DEBUG):
            s3runner._process_messages()

        res = caplog.text.strip().split('\n')
        assert 'Create messages for recent files...' in res[0]
        assert "time_back = {'minutes': 2}" in res[1]

    @mock.patch('s3fs.S3FileSystem')
    @mock.patch('pytroll_collectors.s3stalker_daemon_runner.create_publisher_from_dict_config')
    def test_fetch_new_files_publishes_messages(self, create_publisher, s3_fs):
        """Test process the messages."""
        s3_fs.return_value.ls.return_value = self.ls_output[:2]
        s3_fs.return_value.to_json.return_value = fs_json
        publisher = FakePublish("fake_publisher")
        create_publisher.return_value = publisher
        before_files_arrived = datetime.datetime(2022, 12, 20, 12, 0, 0, tzinfo=UTC)
        from pytroll_collectors.s3stalker import set_last_fetch
        set_last_fetch(before_files_arrived)

        stalker_config = S3_STALKER_CONFIG.copy()
        stalker_config["timedelta"] = dict(seconds=.5)

        startup_timedelta_seconds = 3600
        from datetime import timedelta
        s3runner = S3StalkerRunner(self.bucket, stalker_config, startup_timedelta_seconds)
        try:

            with freeze_time(before_files_arrived + timedelta(hours=1)):
                s3runner.start()
                import time
                time.sleep(.1)
                assert len(publisher.messages_sent) == 2
                first_messages_sent = publisher.messages_sent
                publisher.clear_sent_messages()
            with freeze_time(before_files_arrived + timedelta(hours=2)):
                s3_fs.return_value.ls.return_value = self.ls_output
                time.sleep(.4)
                assert len(publisher.messages_sent) == 2
                assert publisher.messages_sent != first_messages_sent
        finally:
            s3runner.close()
