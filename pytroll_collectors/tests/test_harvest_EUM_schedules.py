"""Unit testing harvest_EUM_schedules module."""

import datetime
import logging
import os
from unittest import mock

import pytest

from pytroll_collectors.harvest_EUM_schedules import harvest_schedules, _generate_pass_list_file_name, _parse_schedules


class FakeResponse:
    def __init__(self, data):
        self.data = data.split(b'\n')

    def readlines(self):
        self.status = 200 if self.data is not None else 404
        return self.data

    def close(self):
        pass


fake_test_pass_file = (b"EARS-VIIRS Pass Predictions\n"
                       b"2019/12/16 05:08:01\n"
                       b"scheduleBeginEumetsat,scheduleEndEumetsat,Satellite\n"
                       b"2019-12-16 00:24,2019-12-16 00:51,npp\n"
                       b"2019-12-16 01:14,2019-12-16 01:44,noaa20\n"
                       b"2019-12-16 02:05,2019-12-16 02:35,npp\n"
                       b"2019-12-16 02:55,2019-12-16 03:25,noaa20\n"
                       b"2019-12-16 03:46,2019-12-16 04:13,npp\n"
                       b"2019-12-16 04:36,2019-12-16 04:56,noaa20\n"
                       b"2019-12-16 05:26,2019-12-16 05:47,npp\n"
                       b"2019-12-16 06:16,2019-12-16 06:37,noaa20\n"
                       b"2019-12-16 07:05,2019-12-16 07:27,npp\n"
                       b"2019-12-16 07:55,2019-12-16 08:16,noaa20\n"
                       b"2019-12-16 08:46,2019-12-16 09:06,npp\n"
                       b"2019-12-16 09:29,2019-12-16 09:55,noaa20\n"
                       b"2019-12-16 10:17,2019-12-16 10:44,npp\n"
                       b"2019-12-16 11:06,2019-12-16 11:34,noaa20\n"
                       b"2019-12-16 11:57,2019-12-16 12:24,npp\n"
                       b"2019-12-16 12:48,2019-12-16 13:15,noaa20\n"
                       b"2019-12-16 13:37,2019-12-16 14:05,npp\n"
                       b"2019-12-16 14:26,2019-12-16 14:56,noaa20\n"
                       b"2019-12-16 15:18,2019-12-16 15:47,npp\n"
                       b"2019-12-16 16:19,2019-12-16 16:37,noaa20\n"
                       b"2019-12-16 17:10,2019-12-16 17:30,npp\n"
                       b"2019-12-16 18:01,2019-12-16 18:20,noaa20\n"
                       b"2019-12-16 18:55,2019-12-16 19:12,npp\n"
                       b"2019-12-16 19:51,2019-12-16 20:03,noaa20\n"
                       b"2019-12-16 20:42,2019-12-16 20:56,npp\n"
                       b"2019-12-16 21:33,2019-12-16 21:47,noaa20\n"
                       b"2019-12-16 22:24,2019-12-16 22:50,npp\n"
                       b"2019-12-16 23:14,2019-12-16 23:41,noaa20\n")


@pytest.fixture
def granule_metadata():
    """Get granule metadata."""
    return {
        'origin': '157.249.16.188:9063', 'sensor': ['viirs'], 'end_decimal': 3, 'stream': 'eumetcast',
        'format': 'SDR_compact', 'orig_platform_name': 'npp',
        'start_time': datetime.datetime(2019, 12, 16, 13, 44, 9), 'variant': 'EARS', 'orbit_number': 42154,
        'dataset':
        [{'uri':
          '/data/pytroll/VIIRS-EARS/SVMC_npp_d20191216_t1344091_e1345333_b42154_c20191216135112000242_eum_ops.h5',
          'uid': 'SVMC_npp_d20191216_t1344091_e1345333_b42154_c20191216135112000242_eum_ops.h5'}],
        'start_decimal': 1, 'proctime': '20191216135112000242', 'antenna': 'ears', 'data_processing_level': '1B',
        'tle_platform_name': 'SUOMI NPP', 'platform_name': 'npp',
        'end_time': datetime.datetime(2019, 12, 16, 13, 45, 33), 'type': 'HDF5', 'collection_area_id': 'eurol'}


@pytest.fixture
def params(granule_metadata):
    """Get harvester parameters."""
    planned_granule_times = set([datetime.datetime(2019, 12, 13, 13, 19),
                                 datetime.datetime(2019, 12, 13, 13, 38),
                                 datetime.datetime(2019, 12, 13, 13, 27)])
    return {'granule_metadata': granule_metadata,
            'planned_granule_times': planned_granule_times}


def test_pass_list_file_name(params, tmp_path):
    """Test creating the pass list filename."""
    basedir = str(tmp_path)
    eum, save_file = _generate_pass_list_file_name(params, basedir, 'https://uns.eumetsat.int/downloads/ears/')
    assert eum == 'https://uns.eumetsat.int/downloads/ears/ears_viirs_pass_prediction_19-12-16.txt'
    assert save_file == os.path.join(basedir, 'ears_viirs_pass_prediction_19-12-16.txt')


def test_pass_list_no_sensor(params, tmp_path):
    """Test that no pass list filename is created without a sensor."""
    params['granule_metadata'].pop('sensor')
    eum, save_file = _generate_pass_list_file_name(params, str(tmp_path), 'https://uns.eumetsat.int/downloads/ears/')
    assert eum is None
    assert save_file is None


@mock.patch('pytroll_collectors.harvest_EUM_schedules.urlopen', return_value=FakeResponse(data=fake_test_pass_file))
def test_harvest_schedule(mock_harvest_schedules_urlopen, params, tmp_path, caplog):
    """Test harvesting the schedule, first by downloading and then from the cached file."""
    basedir = str(tmp_path)
    with caplog.at_level(logging.DEBUG):
        harvest_schedules(params, save_basename=basedir)
        logs = [rec.message for rec in caplog.records]
        assert mock_harvest_schedules_urlopen.call_count == 1
        assert "Saving to file" in str(logs)
    with caplog.at_level(logging.DEBUG):
        # Do a second call, should reread the file instead of download.
        min_time, max_time = harvest_schedules(params, save_basename=basedir)
        logs = [rec.message for rec in caplog.records]
        assert min_time is None
        assert max_time is None
        assert "Reading from cached files" in str(logs)


@mock.patch('pytroll_collectors.harvest_EUM_schedules.urlopen')
def test_harvest_schedule_HTTPError(mock_harvest_schedules, params, tmp_path, caplog):
    """Test that a failed download is logged."""
    from urllib.error import HTTPError
    mock_harvest_schedules.side_effect = HTTPError('This failed', 0, '', '', None)

    with caplog.at_level(logging.ERROR):
        min_time, max_time = harvest_schedules(params, save_basename=str(tmp_path))
        logs = str([rec.message for rec in caplog.records])
        assert min_time is None
        assert max_time is None
        assert "Failed to download file:" in logs


def test_parse_schedules(granule_metadata):
    """Test parsing the schedules."""
    planned_granule_times = set([datetime.datetime(2019, 12, 16, 13, 41, 18, 200000),
                                 datetime.datetime(2019, 12, 16, 13, 42, 43, 600000),
                                 datetime.datetime(2019, 12, 16, 13, 44, 9),
                                 datetime.datetime(2019, 12, 16, 13, 45, 34, 400000),
                                 datetime.datetime(2019, 12, 16, 13, 46, 59, 800000),
                                 datetime.datetime(2019, 12, 16, 13, 48, 25, 200000),
                                 datetime.datetime(2019, 12, 16, 13, 49, 50, 600000),
                                 datetime.datetime(2019, 12, 16, 13, 51, 16),
                                 datetime.datetime(2019, 12, 16, 13, 52, 41, 400000),
                                 datetime.datetime(2019, 12, 16, 13, 54, 6, 800000),
                                 datetime.datetime(2019, 12, 16, 13, 55, 32, 200000),
                                 datetime.datetime(2019, 12, 16, 13, 56, 57, 600000)])
    granule_metadata['platform_name'] = 'suomi npp'
    params = {'granule_metadata': granule_metadata,
              'planned_granule_times': planned_granule_times}
    min_times, max_times = _parse_schedules(params, fake_test_pass_file.split(b'\n'))
    assert min_times == datetime.datetime(2019, 12, 16, 13, 37, tzinfo=datetime.timezone.utc)
    assert max_times == datetime.datetime(2019, 12, 16, 14, 5, tzinfo=datetime.timezone.utc)
