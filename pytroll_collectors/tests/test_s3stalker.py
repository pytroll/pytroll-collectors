#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2020, 2022, 2023 Martin Raspaud

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
"""Tests for s3stalker."""

import datetime
import unittest
from unittest import mock
from copy import deepcopy
from freezegun import freeze_time

from dateutil.tz import tzutc
from dateutil import tz as tzone

import logging
import pytroll_collectors.fsspec_to_message

from pytroll_collectors.s3stalker import S3StalkerRunner
from pytroll_collectors.s3stalker import set_last_fetch, get_last_fetch
from pytroll_collectors.s3stalker import _match_files_to_pattern
from pytroll_collectors.s3stalker import create_messages_for_recent_files

FAKE_MESSAGES = []
FAKE_MESSAGES.append("""pytroll://yuhu file unknown@fake.server 2023-02-13T18:24:52.207612 v1.01 application/json {'filesystem': {'cls': 's3fs.core.S3FileSystem', 'protocol': 's3', 'args': [], 'anon': True}, 'uri': 's3://atms-sdr/GATMO_j01_d20221220_t1230560_e1231276_b26363_c20221220124753607778_cspp_dev.h5', 'uid': 's3://atms-sdr/GATMO_j01_d20221220_t1230560_e1231276_b26363_c20221220124753607778_cspp_dev.h5', 'platform_name': 'j01', 'start_time': datetime.datetime(2022, 12, 20, 12, 30, 56), 'frac': '0', 'end_time': datetime.datetime(1900, 1, 1, 12, 31, 27), 'frac_end': '6', 'orbit_number': '26363', 'process_time': '20221220124753607778'}""")  # noqa
FAKE_MESSAGES.append("""pytroll://yuhu file unknown@fake.server 2023-02-13T18:24:52.207612 v1.01 application/json {'filesystem': {'cls': 's3fs.core.S3FileSystem', 'protocol': 's3', 'args': [], 'anon': True}, 'uri': 's3://atms-sdr/GATMO_j01_d20221220_t1231280_e1231596_b26363_c20221220124754976465_cspp_dev.h5', 'uid': 's3://atms-sdr/GATMO_j01_d20221220_t1231280_e1231596_b26363_c20221220124754976465_cspp_dev.h5', 'platform_name': 'j01', 'start_time': datetime.datetime(2022, 12, 20, 12, 31, 28), 'frac': '0', 'end_time': datetime.datetime(1900, 1, 1, 12, 31, 59), 'frac_end': '6', 'orbit_number': '26363', 'process_time': '20221220124754976465'}""")  # noqa


subject = "/my/great/subject/"

zip_pattern = "{platform_name:3s}_OL_2_{datatype_id:_<6s}_{start_time:%Y%m%dT%H%M%S}_{end_time:%Y%m%dT%H%M%S}_{creation_time:%Y%m%dT%H%M%S}_{duration:4d}_{cycle:3d}_{relative_orbit:3d}_{frame:4d}_{centre:3s}_{mode:1s}_{timeliness:2s}_{collection:3s}.zip"  # noqa

ls_output = [{
                 'Key': 'sentinel-s3-ol2wfr-zips/2020/11/21/S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.md5',  # noqa
                 'LastModified': datetime.datetime(2020, 11, 21, 11, 0, 45, 182000, tzinfo=tzutc()),
                 'ETag': '"0e60331d8cd7f2eddcf1f6258cb8e003"', 'Size': 32, 'StorageClass': 'STANDARD', 'type': 'file',
                 'size': 32,
                 'name': 'sentinel-s3-ol2wfr-zips/2020/11/21/S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.md5'},  # noqa
             {
                 'Key': 'sentinel-s3-ol2wfr-zips/2020/11/21/S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.zip',  # noqa
                 'LastModified': datetime.datetime(2020, 11, 21, 11, 0, 45, 101000, tzinfo=tzutc()),
                 'ETag': '"943ff0b74eb26c47ec9b9f7eb8228e58-12"', 'Size': 95852128, 'StorageClass': 'STANDARD',
                 'type': 'file', 'size': 95852128,
                 'name': 'sentinel-s3-ol2wfr-zips/2020/11/21/S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.zip'},  # noqa
             {
                 'Key': 'sentinel-s3-ol2wfr-zips/2020/11/21/S3A_OL_2_WFR____20201121T075933_20201121T080210_20201122T161638_0157_065_192_1980_MAR_O_NT_002.md5',  # noqa
                 'LastModified': datetime.datetime(2020, 11, 22, 17, 0, 48, 63000, tzinfo=tzutc()),
                 'ETag': '"840fcda0950de79200802526e483137a"', 'Size': 32, 'StorageClass': 'STANDARD', 'type': 'file',
                 'size': 32,
                 'name': 'sentinel-s3-ol2wfr-zips/2020/11/21/S3A_OL_2_WFR____20201121T075933_20201121T080210_20201122T161638_0157_065_192_1980_MAR_O_NT_002.md5'},  # noqa
             {
                 'Key': 'sentinel-s3-ol2wfr-zips/2020/11/21/S3A_OL_2_WFR____20201121T075933_20201121T080210_20201122T161638_0157_065_192_1980_MAR_O_NT_002.zip',  # noqa
                 'LastModified': datetime.datetime(2020, 11, 22, 17, 0, 47, 972000, tzinfo=tzutc()),
                 'ETag': '"af148551482712f4ce89c7a955770371-12"', 'Size': 96549881, 'StorageClass': 'STANDARD',
                 'type': 'file', 'size': 96549881,
                 'name': 'sentinel-s3-ol2wfr-zips/2020/11/21/S3A_OL_2_WFR____20201121T075933_20201121T080210_20201122T161638_0157_065_192_1980_MAR_O_NT_002.zip'},  # noqa
             {
                 'Key': 'sentinel-s3-ol2wfr-zips/2020/11/21/S3A_OL_2_WFR____20201121T094032_20201121T094309_20201121T121152_0157_065_193_1980_MAR_O_NR_002.md5',  # noqa
                 'LastModified': datetime.datetime(2020, 11, 21, 13, 0, 45, 300000, tzinfo=tzutc()),
                 'ETag': '"4573a60fbb3a11e1c016060513f257cb"', 'Size': 32, 'StorageClass': 'STANDARD', 'type': 'file',
                 'size': 32,
                 'name': 'sentinel-s3-ol2wfr-zips/2020/11/21/S3A_OL_2_WFR____20201121T094032_20201121T094309_20201121T121152_0157_065_193_1980_MAR_O_NR_002.md5'},  # noqa
             {
                 'Key': 'sentinel-s3-ol2wfr-zips/2020/11/21/S3A_OL_2_WFR____20201121T094032_20201121T094309_20201121T121152_0157_065_193_1980_MAR_O_NR_002.zip',  # noqa
                 'LastModified': datetime.datetime(2020, 11, 21, 13, 0, 45, 203000, tzinfo=tzutc()),
                 'ETag': '"d678c06a32fdbbc805902a70e0dc5fd7-13"', 'Size': 106350831, 'StorageClass': 'STANDARD',
                 'type': 'file', 'size': 106350831,
                 'name': 'sentinel-s3-ol2wfr-zips/2020/11/21/S3A_OL_2_WFR____20201121T094032_20201121T094309_20201121T121152_0157_065_193_1980_MAR_O_NR_002.zip'},  # noqa
             {
                 'Key': 'sentinel-s3-ol2wfr-zips/2020/11/21/S3A_OL_2_WFR____20201121T094032_20201121T094309_20201122T183833_0157_065_193_1980_MAR_O_NT_002.md5',  # noqa
                 'LastModified': datetime.datetime(2020, 11, 22, 20, 0, 48, 484000, tzinfo=tzutc()),
                 'ETag': '"1ea87d20cda00c5a265242f9ee2186d9"', 'Size': 32, 'StorageClass': 'STANDARD', 'type': 'file',
                 'size': 32,
                 'name': 'sentinel-s3-ol2wfr-zips/2020/11/21/S3A_OL_2_WFR____20201121T094032_20201121T094309_20201122T183833_0157_065_193_1980_MAR_O_NT_002.md5'},  # noqa
             {
                 'Key': 'sentinel-s3-ol2wfr-zips/2020/11/21/S3A_OL_2_WFR____20201121T094032_20201121T094309_20201122T183833_0157_065_193_1980_MAR_O_NT_002.zip',  # noqa
                 'LastModified': datetime.datetime(2020, 11, 22, 20, 0, 48, 399000, tzinfo=tzutc()),
                 'ETag': '"0980f728ca3add53f6750a9dd255dcd0-13"', 'Size': 105844797, 'StorageClass': 'STANDARD',
                 'type': 'file', 'size': 105844797,
                 'name': 'sentinel-s3-ol2wfr-zips/2020/11/21/S3A_OL_2_WFR____20201121T094032_20201121T094309_20201122T183833_0157_065_193_1980_MAR_O_NT_002.zip'},  # noqa
             {
                 'Key': 'sentinel-s3-ol2wfr-zips/2020/11/21/S3B_OL_2_WFR____20201121T090112_20201121T090349_20201121T113158_0156_046_050_1980_MAR_O_NR_002.md5',  # noqa
                 'LastModified': datetime.datetime(2020, 11, 21, 12, 0, 45, 666000, tzinfo=tzutc()),
                 'ETag': '"90b99dff8aac7dfc7f0a040ef4bb360a"', 'Size': 32, 'StorageClass': 'STANDARD', 'type': 'file',
                 'size': 32,
                 'name': 'sentinel-s3-ol2wfr-zips/2020/11/21/S3B_OL_2_WFR____20201121T090112_20201121T090349_20201121T113158_0156_046_050_1980_MAR_O_NR_002.md5'},  # noqa
             {
                 'Key': 'sentinel-s3-ol2wfr-zips/2020/11/21/S3B_OL_2_WFR____20201121T090112_20201121T090349_20201121T113158_0156_046_050_1980_MAR_O_NR_002.zip',  # noqa
                 'LastModified': datetime.datetime(2020, 11, 21, 12, 0, 45, 584000, tzinfo=tzutc()),
                 'ETag': '"ef9252032d7d04cbe91c0da2321d9fff-11"', 'Size': 89658713, 'StorageClass': 'STANDARD',
                 'type': 'file', 'size': 89658713,
                 'name': 'sentinel-s3-ol2wfr-zips/2020/11/21/S3B_OL_2_WFR____20201121T090112_20201121T090349_20201121T113158_0156_046_050_1980_MAR_O_NR_002.zip'},  # noqa
             {
                 'Key': 'sentinel-s3-ol2wfr-zips/2020/11/21/S3B_OL_2_WFR____20201121T090112_20201121T090349_20201122T155502_0156_046_050_1980_MAR_O_NT_002.md5',  # noqa
                 'LastModified': datetime.datetime(2020, 11, 22, 17, 0, 48, 323000, tzinfo=tzutc()),
                 'ETag': '"683a007042616f451b6a4e2ce0054cd5"', 'Size': 32, 'StorageClass': 'STANDARD', 'type': 'file',
                 'size': 32,
                 'name': 'sentinel-s3-ol2wfr-zips/2020/11/21/S3B_OL_2_WFR____20201121T090112_20201121T090349_20201122T155502_0156_046_050_1980_MAR_O_NT_002.md5'},  # noqa
             {
                 'Key': 'sentinel-s3-ol2wfr-zips/2020/11/21/S3B_OL_2_WFR____20201121T090112_20201121T090349_20201122T155502_0156_046_050_1980_MAR_O_NT_002.zip',  # noqa
                 'LastModified': datetime.datetime(2020, 11, 22, 17, 0, 48, 242000, tzinfo=tzutc()),
                 'ETag': '"c2160a30bea804eb49e6dbb514b5333e-11"', 'Size': 89168852, 'StorageClass': 'STANDARD',
                 'type': 'file', 'size': 89168852,
                 'name': 'sentinel-s3-ol2wfr-zips/2020/11/21/S3B_OL_2_WFR____20201121T090112_20201121T090349_20201122T155502_0156_046_050_1980_MAR_O_NT_002.zip'},  # noqa
             {
                 'Key': 'sentinel-s3-ol2wfr-zips/2020/11/21/S3B_OL_2_WFR____20201121T104211_20201121T104448_20201121T131528_0157_046_051_1980_MAR_O_NR_002.md5',  # noqa
                 'LastModified': datetime.datetime(2020, 11, 21, 14, 1, 9, 82000, tzinfo=tzutc()),
                 'ETag': '"5ab58c472c32bbff54aadbadc4aeb93c"', 'Size': 32, 'StorageClass': 'STANDARD', 'type': 'file',
                 'size': 32,
                 'name': 'sentinel-s3-ol2wfr-zips/2020/11/21/S3B_OL_2_WFR____20201121T104211_20201121T104448_20201121T131528_0157_046_051_1980_MAR_O_NR_002.md5'},  # noqa
             {
                 'Key': 'sentinel-s3-ol2wfr-zips/2020/11/21/S3B_OL_2_WFR____20201121T104211_20201121T104448_20201121T131528_0157_046_051_1980_MAR_O_NR_002.zip',  # noqa
                 'LastModified': datetime.datetime(2020, 11, 21, 14, 1, 8, 996000, tzinfo=tzutc()),
                 'ETag': '"0251f232014b24bacc3d45c10c4c0df2-38"', 'Size': 315207067,
                 'StorageClass': 'STANDARD',
                 'type': 'file', 'size': 315207067,
                 'name': 'sentinel-s3-ol2wfr-zips/2020/11/21/S3B_OL_2_WFR____20201121T104211_20201121T104448_20201121T131528_0157_046_051_1980_MAR_O_NR_002.zip'},  # noqa
             {
                 'Key': 'sentinel-s3-ol2wfr-zips/2020/11/21/S3B_OL_2_WFR____20201121T104211_20201121T104448_20201122T192710_0157_046_051_1980_MAR_O_NT_002.md5',  # noqa
                 'LastModified': datetime.datetime(2020, 11, 22, 21, 1, 5, 181000, tzinfo=tzutc()),
                 'ETag': '"c5e3e19f37b3670d4b4792d430e2a3a6"', 'Size': 32,
                 'StorageClass': 'STANDARD', 'type': 'file',
                 'size': 32,
                 'name': 'sentinel-s3-ol2wfr-zips/2020/11/21/S3B_OL_2_WFR____20201121T104211_20201121T104448_20201122T192710_0157_046_051_1980_MAR_O_NT_002.md5'},  # noqa
             {
                 'Key': 'sentinel-s3-ol2wfr-zips/2020/11/21/S3B_OL_2_WFR____20201121T104211_20201121T104448_20201122T192710_0157_046_051_1980_MAR_O_NT_002.zip',  # noqa
                 'LastModified': datetime.datetime(2020, 11, 22, 21, 1, 5, 99000, tzinfo=tzutc()),
                 'ETag': '"d043d88fe0f2b27f58e4993fef8017d1-38"', 'Size': 314382569,
                 'StorageClass': 'STANDARD',
                 'type': 'file', 'size': 314382569,
                 'name': 'sentinel-s3-ol2wfr-zips/2020/11/21/S3B_OL_2_WFR____20201121T104211_20201121T104448_20201122T192710_0157_046_051_1980_MAR_O_NT_002.zip'}]  # noqa

fs_json = '{"cls": "s3fs.core.S3FileSystem", "protocol": "s3", "args": [], "anon": true}'

zip_json = '{"cls": "fsspec.implementations.zip.ZipFileSystem", "protocol": "abstract", "args": ["sentinel-s3-ol2wfr-zips/2020/11/21/S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.zip"], "target_protocol": "s3", "target_options": {"anon": true, "client_kwargs": {}}}'  # noqa
zip_json_fo = '{"cls": "fsspec.implementations.zip.ZipFileSystem", "protocol": "abstract", "fo": "sentinel-s3-ol2wfr-zips/2020/11/21/S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.zip", "target_protocol": "s3", "target_options": {"anon": true, "client_kwargs": {}}}'  # noqa

zip_content = {
    'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa01_reflectance.nc': {  # noqa
        'orig_filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa01_reflectance.nc',  # noqa
        'filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa01_reflectance.nc',  # noqa
        'date_time': (2020, 11, 21, 10, 39, 46),
        'compress_type': 0,
        '_compresslevel': None,
        'comment': b'',
        'extra': b'UT\x05\x00\x03q\xee\xb8_ux\x0b\x00\x01\x04\xf4\x01\x00\x00\x04\xf4\x01\x00\x00',
        'create_system': 3,
        'create_version': 30,
        'extract_version': 10,
        'reserved': 0,
        'flag_bits': 0,
        'volume': 0,
        'internal_attr': 0,
        'external_attr': 2175008768,
        'header_offset': 30934040,
        'CRC': 2851413882,
        'compress_size': 812447,
        'file_size': 812447,
        '_raw_time': 21751,
        'name': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa01_reflectance.nc',  # noqa
        'size': 812447,
        'type': 'file'},
    'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa02_reflectance.nc': {  # noqa
        'orig_filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa02_reflectance.nc',  # noqa
        'filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa02_reflectance.nc',  # noqa
        'date_time': (2020, 11, 21, 10, 39, 46),
        'compress_type': 0,
        '_compresslevel': None,
        'comment': b'',
        'extra': b'UT\x05\x00\x03q\xee\xb8_ux\x0b\x00\x01\x04\xf4\x01\x00\x00\x04\xf4\x01\x00\x00',
        'create_system': 3,
        'create_version': 30,
        'extract_version': 10,
        'reserved': 0,
        'flag_bits': 0,
        'volume': 0,
        'internal_attr': 0,
        'external_attr': 2175008768,
        'header_offset': 95035802,
        'CRC': 1550770338,
        'compress_size': 810018,
        'file_size': 810018,
        '_raw_time': 21751,
        'name': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa02_reflectance.nc',  # noqa
        'size': 810018,
        'type': 'file'},
    'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa03_reflectance.nc': {  # noqa
        'orig_filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa03_reflectance.nc',  # noqa
        'filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa03_reflectance.nc',  # noqa
        'date_time': (2020, 11, 21, 10, 39, 46),
        'compress_type': 0,
        '_compresslevel': None,
        'comment': b'',
        'extra': b'UT\x05\x00\x03r\xee\xb8_ux\x0b\x00\x01\x04\xf4\x01\x00\x00\x04\xf4\x01\x00\x00',
        'create_system': 3,
        'create_version': 30,
        'extract_version': 10,
        'reserved': 0,
        'flag_bits': 0,
        'volume': 0,
        'internal_attr': 0,
        'external_attr': 2175008768,
        'header_offset': 30131547,
        'CRC': 2216481454,
        'compress_size': 802316,
        'file_size': 802316,
        '_raw_time': 21751,
        'name': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa03_reflectance.nc',  # noqa
        'size': 802316,
        'type': 'file'},
    'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa04_reflectance.nc': {  # noqa
        'orig_filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa04_reflectance.nc',  # noqa
        'filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa04_reflectance.nc',  # noqa
        'date_time': (2020, 11, 21, 10, 39, 46),
        'compress_type': 0,
        '_compresslevel': None,
        'comment': b'',
        'extra': b'UT\x05\x00\x03q\xee\xb8_ux\x0b\x00\x01\x04\xf4\x01\x00\x00\x04\xf4\x01\x00\x00',
        'create_system': 3,
        'create_version': 30,
        'extract_version': 10,
        'reserved': 0,
        'flag_bits': 0,
        'volume': 0,
        'internal_attr': 0,
        'external_attr': 2175008768,
        'header_offset': 158,
        'CRC': 359723849,
        'compress_size': 792032,
        'file_size': 792032,
        '_raw_time': 21751,
        'name': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa04_reflectance.nc',  # noqa
        'size': 792032,
        'type': 'file'},
    'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa05_reflectance.nc': {  # noqa
        'orig_filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa05_reflectance.nc',  # noqa
        'filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa05_reflectance.nc',  # noqa
        'date_time': (2020, 11, 21, 10, 39, 46),
        'compress_type': 0,
        '_compresslevel': None,
        'comment': b'',
        'extra': b'UT\x05\x00\x03r\xee\xb8_ux\x0b\x00\x01\x04\xf4\x01\x00\x00\x04\xf4\x01\x00\x00',
        'create_system': 3,
        'create_version': 30,
        'extract_version': 10,
        'reserved': 0,
        'flag_bits': 0,
        'volume': 0,
        'internal_attr': 0,
        'external_attr': 2175008768,
        'header_offset': 29342629,
        'CRC': 2288634885,
        'compress_size': 788741,
        'file_size': 788741,
        '_raw_time': 21751,
        'name': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa05_reflectance.nc',  # noqa
        'size': 788741,
        'type': 'file'},
    'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa06_reflectance.nc': {  # noqa
        'orig_filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa06_reflectance.nc',  # noqa
        'filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa06_reflectance.nc',  # noqa
        'date_time': (2020, 11, 21, 10, 39, 46),
        'compress_type': 0,
        '_compresslevel': None,
        'comment': b'',
        'extra': b'UT\x05\x00\x03q\xee\xb8_ux\x0b\x00\x01\x04\xf4\x01\x00\x00\x04\xf4\x01\x00\x00',
        'create_system': 3,
        'create_version': 30,
        'extract_version': 10,
        'reserved': 0,
        'flag_bits': 0,
        'volume': 0,
        'internal_attr': 0,
        'external_attr': 2175008768,
        'header_offset': 93483295,
        'CRC': 2172930219,
        'compress_size': 782848,
        'file_size': 782848,
        '_raw_time': 21751,
        'name': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa06_reflectance.nc',  # noqa
        'size': 782848,
        'type': 'file'},
    'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa07_reflectance.nc': {  # noqa
        'orig_filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa07_reflectance.nc',  # noqa
        'filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa07_reflectance.nc',  # noqa
        'date_time': (2020, 11, 21, 10, 39, 46),
        'compress_type': 0,
        '_compresslevel': None,
        'comment': b'',
        'extra': b'UT\x05\x00\x03r\xee\xb8_ux\x0b\x00\x01\x04\xf4\x01\x00\x00\x04\xf4\x01\x00\x00',
        'create_system': 3,
        'create_version': 30,
        'extract_version': 10,
        'reserved': 0,
        'flag_bits': 0,
        'volume': 0,
        'internal_attr': 0,
        'external_attr': 2175008768,
        'header_offset': 22674300,
        'CRC': 2082896662,
        'compress_size': 774239,
        'file_size': 774239,
        '_raw_time': 21751,
        'name': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa07_reflectance.nc',  # noqa
        'size': 774239,
        'type': 'file'},
    'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa08_reflectance.nc': {  # noqa
        'orig_filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa08_reflectance.nc',  # noqa
        'filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa08_reflectance.nc',  # noqa
        'date_time': (2020, 11, 21, 10, 39, 46),
        'compress_type': 0,
        '_compresslevel': None,
        'comment': b'',
        'extra': b'UT\x05\x00\x03q\xee\xb8_ux\x0b\x00\x01\x04\xf4\x01\x00\x00\x04\xf4\x01\x00\x00',
        'create_system': 3,
        'create_version': 30,
        'extract_version': 10,
        'reserved': 0,
        'flag_bits': 0,
        'volume': 0,
        'internal_attr': 0,
        'external_attr': 2175008768,
        'header_offset': 32483839,
        'CRC': 2347470611,
        'compress_size': 769323,
        'file_size': 769323,
        '_raw_time': 21751,
        'name': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa08_reflectance.nc',  # noqa
        'size': 769323,
        'type': 'file'},
    'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa09_reflectance.nc': {  # noqa
        'orig_filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa09_reflectance.nc',  # noqa
        'filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa09_reflectance.nc',  # noqa
        'date_time': (2020, 11, 21, 10, 39, 46),
        'compress_type': 0,
        '_compresslevel': None,
        'comment': b'',
        'extra': b'UT\x05\x00\x03q\xee\xb8_ux\x0b\x00\x01\x04\xf4\x01\x00\x00\x04\xf4\x01\x00\x00',
        'create_system': 3,
        'create_version': 30,
        'extract_version': 10,
        'reserved': 0,
        'flag_bits': 0,
        'volume': 0,
        'internal_attr': 0,
        'external_attr': 2175008768,
        'header_offset': 94266320,
        'CRC': 1566577714,
        'compress_size': 769305,
        'file_size': 769305,
        '_raw_time': 21751,
        'name': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa09_reflectance.nc',  # noqa
        'size': 769305,
        'type': 'file'},
    'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa10_reflectance.nc': {  # noqa
        'orig_filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa10_reflectance.nc',  # noqa
        'filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa10_reflectance.nc',  # noqa
        'date_time': (2020, 11, 21, 10, 39, 46),
        'compress_type': 0,
        '_compresslevel': None,
        'comment': b'',
        'extra': b'UT\x05\x00\x03q\xee\xb8_ux\x0b\x00\x01\x04\xf4\x01\x00\x00\x04\xf4\x01\x00\x00',
        'create_system': 3,
        'create_version': 30,
        'extract_version': 10,
        'reserved': 0,
        'flag_bits': 0,
        'volume': 0,
        'internal_attr': 0,
        'external_attr': 2175008768,
        'header_offset': 37731858,
        'CRC': 2131974571,
        'compress_size': 769798,
        'file_size': 769798,
        '_raw_time': 21751,
        'name': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa10_reflectance.nc',  # noqa
        'size': 769798,
        'type': 'file'},
    'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa11_reflectance.nc': {  # noqa
        'orig_filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa11_reflectance.nc',  # noqa
        'filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa11_reflectance.nc',  # noqa
        'date_time': (2020, 11, 21, 10, 39, 46),
        'compress_type': 0,
        '_compresslevel': None,
        'comment': b'',
        'extra': b'UT\x05\x00\x03q\xee\xb8_ux\x0b\x00\x01\x04\xf4\x01\x00\x00\x04\xf4\x01\x00\x00',
        'create_system': 3,
        'create_version': 30,
        'extract_version': 10,
        'reserved': 0,
        'flag_bits': 0,
        'volume': 0,
        'internal_attr': 0,
        'external_attr': 2175008768,
        'header_offset': 36967691,
        'CRC': 1152146405,
        'compress_size': 763990,
        'file_size': 763990,
        '_raw_time': 21751,
        'name': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa11_reflectance.nc',  # noqa
        'size': 763990,
        'type': 'file'},
    'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa12_reflectance.nc': {  # noqa
        'orig_filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa12_reflectance.nc',  # noqa
        'filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa12_reflectance.nc',  # noqa
        'date_time': (2020, 11, 21, 10, 39, 46),
        'compress_type': 0,
        '_compresslevel': None,
        'comment': b'',
        'extra': b'UT\x05\x00\x03q\xee\xb8_ux\x0b\x00\x01\x04\xf4\x01\x00\x00\x04\xf4\x01\x00\x00',
        'create_system': 3,
        'create_version': 30,
        'extract_version': 10,
        'reserved': 0,
        'flag_bits': 0,
        'volume': 0,
        'internal_attr': 0,
        'external_attr': 2175008768,
        'header_offset': 26249438,
        'CRC': 2965276564,
        'compress_size': 755485,
        'file_size': 755485,
        '_raw_time': 21751,
        'name': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa12_reflectance.nc',  # noqa
        'size': 755485,
        'type': 'file'},
    'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa16_reflectance.nc': {  # noqa
        'orig_filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa16_reflectance.nc',  # noqa
        'filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa16_reflectance.nc',  # noqa
        'date_time': (2020, 11, 21, 10, 39, 46),
        'compress_type': 0,
        '_compresslevel': None,
        'comment': b'',
        'extra': b'UT\x05\x00\x03q\xee\xb8_ux\x0b\x00\x01\x04\xf4\x01\x00\x00\x04\xf4\x01\x00\x00',
        'create_system': 3,
        'create_version': 30,
        'extract_version': 10,
        'reserved': 0,
        'flag_bits': 0,
        'volume': 0,
        'internal_attr': 0,
        'external_attr': 2175008768,
        'header_offset': 3483836,
        'CRC': 1387370809,
        'compress_size': 698176,
        'file_size': 698176,
        '_raw_time': 21751,
        'name': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa16_reflectance.nc',  # noqa
        'size': 698176,
        'type': 'file'},
    'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa17_reflectance.nc': {  # noqa
        'orig_filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa17_reflectance.nc',  # noqa
        'filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa17_reflectance.nc',  # noqa
        'date_time': (2020, 11, 21, 10, 39, 46),
        'compress_type': 0,
        '_compresslevel': None,
        'comment': b'',
        'extra': b'UT\x05\x00\x03q\xee\xb8_ux\x0b\x00\x01\x04\xf4\x01\x00\x00\x04\xf4\x01\x00\x00',
        'create_system': 3,
        'create_version': 30,
        'extract_version': 10,
        'reserved': 0,
        'flag_bits': 0,
        'volume': 0,
        'internal_attr': 0,
        'external_attr': 2175008768,
        'header_offset': 23448716,
        'CRC': 317052572,
        'compress_size': 681543,
        'file_size': 681543,
        '_raw_time': 21751,
        'name': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa17_reflectance.nc',  # noqa
        'size': 681543,
        'type': 'file'},
    'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa18_reflectance.nc': {  # noqa
        'orig_filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa18_reflectance.nc',  # noqa
        'filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa18_reflectance.nc',  # noqa
        'date_time': (2020, 11, 21, 10, 39, 46),
        'compress_type': 0,
        '_compresslevel': None,
        'comment': b'',
        'extra': b'UT\x05\x00\x03q\xee\xb8_ux\x0b\x00\x01\x04\xf4\x01\x00\x00\x04\xf4\x01\x00\x00',
        'create_system': 3,
        'create_version': 30,
        'extract_version': 10,
        'reserved': 0,
        'flag_bits': 0,
        'volume': 0,
        'internal_attr': 0,
        'external_attr': 2175008768,
        'header_offset': 4952384,
        'CRC': 2759856937,
        'compress_size': 686983,
        'file_size': 686983,
        '_raw_time': 21751,
        'name': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa18_reflectance.nc',  # noqa
        'size': 686983,
        'type': 'file'},
    'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa21_reflectance.nc': {  # noqa
        'orig_filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa21_reflectance.nc',  # noqa
        'filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa21_reflectance.nc',  # noqa
        'date_time': (2020, 11, 21, 10, 39, 46),
        'compress_type': 0,
        '_compresslevel': None,
        'comment': b'',
        'extra': b'UT\x05\x00\x03q\xee\xb8_ux\x0b\x00\x01\x04\xf4\x01\x00\x00\x04\xf4\x01\x00\x00',
        'create_system': 3,
        'create_version': 30,
        'extract_version': 10,
        'reserved': 0,
        'flag_bits': 0,
        'volume': 0,
        'internal_attr': 0,
        'external_attr': 2175008768,
        'header_offset': 792367,
        'CRC': 602824357,
        'compress_size': 789560,
        'file_size': 789560,
        '_raw_time': 21751,
        'name': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/Oa21_reflectance.nc',  # noqa
        'size': 789560,
        'type': 'file'},
    'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/chl_nn.nc': {
        'orig_filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/chl_nn.nc',  # noqa
        'filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/chl_nn.nc',  # noqa
        'date_time': (2020, 11, 21, 10, 39, 46),
        'compress_type': 0,
        '_compresslevel': None,
        'comment': b'',
        'extra': b'UT\x05\x00\x03q\xee\xb8_ux\x0b\x00\x01\x04\xf4\x01\x00\x00\x04\xf4\x01\x00\x00',
        'create_system': 3,
        'create_version': 30,
        'extract_version': 10,
        'reserved': 0,
        'flag_bits': 0,
        'volume': 0,
        'internal_attr': 0,
        'external_attr': 2175008768,
        'header_offset': 32083650,
        'CRC': 4191028762,
        'compress_size': 400022,
        'file_size': 400022,
        '_raw_time': 21751,
        'name': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/chl_nn.nc',  # noqa
        'size': 400022,
        'type': 'file'},
    'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/chl_oc4me.nc': {  # noqa
        'orig_filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/chl_oc4me.nc',  # noqa
        'filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/chl_oc4me.nc',  # noqa
        'date_time': (2020, 11, 21, 10, 39, 46),
        'compress_type': 0,
        '_compresslevel': None,
        'comment': b'',
        'extra': b'UT\x05\x00\x03r\xee\xb8_ux\x0b\x00\x01\x04\xf4\x01\x00\x00\x04\xf4\x01\x00\x00',
        'create_system': 3,
        'create_version': 30,
        'extract_version': 10,
        'reserved': 0,
        'flag_bits': 0,
        'volume': 0,
        'internal_attr': 0,
        'external_attr': 2175008768,
        'header_offset': 4182189,
        'CRC': 4054841977,
        'compress_size': 364463,
        'file_size': 364463,
        '_raw_time': 21751,
        'name': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/chl_oc4me.nc',  # noqa
        'size': 364463,
        'type': 'file'},
    'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/geo_coordinates.nc': {  # noqa
        'orig_filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/geo_coordinates.nc',  # noqa
        'filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/geo_coordinates.nc',  # noqa
        'date_time': (2020, 11, 21, 10, 39, 46),
        'compress_type': 0,
        '_compresslevel': None,
        'comment': b'',
        'extra': b'UT\x05\x00\x03q\xee\xb8_ux\x0b\x00\x01\x04\xf4\x01\x00\x00\x04\xf4\x01\x00\x00',
        'create_system': 3,
        'create_version': 30,
        'extract_version': 10,
        'reserved': 0,
        'flag_bits': 0,
        'volume': 0,
        'internal_attr': 0,
        'external_attr': 2175008768,
        'header_offset': 38501833,
        'CRC': 1552456915,
        'compress_size': 54981286,
        'file_size': 54981286,
        '_raw_time': 21751,
        'name': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/geo_coordinates.nc',  # noqa
        'size': 54981286,
        'type': 'file'},
    'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/instrument_data.nc': {  # noqa
        'orig_filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/instrument_data.nc',  # noqa
        'filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/instrument_data.nc',  # noqa
        'date_time': (2020, 11, 21, 10, 39, 46),
        'compress_type': 0,
        '_compresslevel': None,
        'comment': b'',
        'extra': b'UT\x05\x00\x03q\xee\xb8_ux\x0b\x00\x01\x04\xf4\x01\x00\x00\x04\xf4\x01\x00\x00',
        'create_system': 3,
        'create_version': 30,
        'extract_version': 10,
        'reserved': 0,
        'flag_bits': 0,
        'volume': 0,
        'internal_attr': 0,
        'external_attr': 2175008768,
        'header_offset': 33484874,
        'CRC': 176564353,
        'compress_size': 963901,
        'file_size': 963901,
        '_raw_time': 21751,
        'name': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/instrument_data.nc',  # noqa
        'size': 963901,
        'type': 'file'},
    'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/iop_nn.nc': {
        'orig_filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/iop_nn.nc',  # noqa
        'filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/iop_nn.nc',  # noqa
        'date_time': (2020, 11, 21, 10, 39, 46),
        'compress_type': 0,
        '_compresslevel': None,
        'comment': b'',
        'extra': b'UT\x05\x00\x03q\xee\xb8_ux\x0b\x00\x01\x04\xf4\x01\x00\x00\x04\xf4\x01\x00\x00',
        'create_system': 3,
        'create_version': 30,
        'extract_version': 10,
        'reserved': 0,
        'flag_bits': 0,
        'volume': 0,
        'internal_attr': 0,
        'external_attr': 2175008768,
        'header_offset': 4546822,
        'CRC': 2910436099,
        'compress_size': 405395,
        'file_size': 405395,
        '_raw_time': 21751,
        'name': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/iop_nn.nc',  # noqa
        'size': 405395,
        'type': 'file'},
    'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/iwv.nc': {
        'orig_filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/iwv.nc',  # noqa
        'filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/iwv.nc',  # noqa
        'date_time': (2020, 11, 21, 10, 39, 46),
        'compress_type': 0,
        '_compresslevel': None,
        'comment': b'',
        'extra': b'UT\x05\x00\x03r\xee\xb8_ux\x0b\x00\x01\x04\xf4\x01\x00\x00\x04\xf4\x01\x00\x00',
        'create_system': 3,
        'create_version': 30,
        'extract_version': 10,
        'reserved': 0,
        'flag_bits': 0,
        'volume': 0,
        'internal_attr': 0,
        'external_attr': 2175008768,
        'header_offset': 27005100,
        'CRC': 1231211298,
        'compress_size': 2337365,
        'file_size': 2337365,
        '_raw_time': 21751,
        'name': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/iwv.nc',  # noqa
        'size': 2337365,
        'type': 'file'},
    'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/par.nc': {
        'orig_filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/par.nc',  # noqa
        'filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/par.nc',  # noqa
        'date_time': (2020, 11, 21, 10, 39, 46),
        'compress_type': 0,
        '_compresslevel': None,
        'comment': b'',
        'extra': b'UT\x05\x00\x03q\xee\xb8_ux\x0b\x00\x01\x04\xf4\x01\x00\x00\x04\xf4\x01\x00\x00',
        'create_system': 3,
        'create_version': 30,
        'extract_version': 10,
        'reserved': 0,
        'flag_bits': 0,
        'volume': 0,
        'internal_attr': 0,
        'external_attr': 2175008768,
        'header_offset': 33253339,
        'CRC': 1297297518,
        'compress_size': 231371,
        'file_size': 231371,
        '_raw_time': 21751,
        'name': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/par.nc',  # noqa
        'size': 231371,
        'type': 'file'},
    'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/tie_geo_coordinates.nc': {  # noqa
        'orig_filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/tie_geo_coordinates.nc',  # noqa
        'filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/tie_geo_coordinates.nc',  # noqa
        'date_time': (2020, 11, 21, 10, 39, 46),
        'compress_type': 0,
        '_compresslevel': None,
        'comment': b'',
        'extra': b'UT\x05\x00\x03q\xee\xb8_ux\x0b\x00\x01\x04\xf4\x01\x00\x00\x04\xf4\x01\x00\x00',
        'create_system': 3,
        'create_version': 30,
        'extract_version': 10,
        'reserved': 0,
        'flag_bits': 0,
        'volume': 0,
        'internal_attr': 0,
        'external_attr': 2175008768,
        'header_offset': 24706923,
        'CRC': 3545277810,
        'compress_size': 1123313,
        'file_size': 1123313,
        '_raw_time': 21751,
        'name': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/tie_geo_coordinates.nc',  # noqa
        'size': 1123313,
        'type': 'file'},
    'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/tie_geometries.nc': {  # noqa
        'orig_filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/tie_geometries.nc',  # noqa
        'filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/tie_geometries.nc',  # noqa
        'date_time': (2020, 11, 21, 10, 39, 46),
        'compress_type': 0,
        '_compresslevel': None,
        'comment': b'',
        'extra': b'UT\x05\x00\x03r\xee\xb8_ux\x0b\x00\x01\x04\xf4\x01\x00\x00\x04\xf4\x01\x00\x00',
        'create_system': 3,
        'create_version': 30,
        'extract_version': 10,
        'reserved': 0,
        'flag_bits': 0,
        'volume': 0,
        'internal_attr': 0,
        'external_attr': 2175008768,
        'header_offset': 1582104,
        'CRC': 2917373038,
        'compress_size': 1901557,
        'file_size': 1901557,
        '_raw_time': 21751,
        'name': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/tie_geometries.nc',  # noqa
        'size': 1901557,
        'type': 'file'},
    'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/tie_meteo.nc': {  # noqa
        'orig_filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/tie_meteo.nc',  # noqa
        'filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/tie_meteo.nc',  # noqa
        'date_time': (2020, 11, 21, 10, 39, 46),
        'compress_type': 0,
        '_compresslevel': None,
        'comment': b'',
        'extra': b'UT\x05\x00\x03q\xee\xb8_ux\x0b\x00\x01\x04\xf4\x01\x00\x00\x04\xf4\x01\x00\x00',
        'create_system': 3,
        'create_version': 30,
        'extract_version': 10,
        'reserved': 0,
        'flag_bits': 0,
        'volume': 0,
        'internal_attr': 0,
        'external_attr': 2175008768,
        'header_offset': 5654911,
        'CRC': 1712320447,
        'compress_size': 17019219,
        'file_size': 17019219,
        '_raw_time': 21751,
        'name': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/tie_meteo.nc',  # noqa
        'size': 17019219,
        'type': 'file'},
    'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/time_coordinates.nc': {  # noqa
        'orig_filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/time_coordinates.nc',  # noqa
        'filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/time_coordinates.nc',  # noqa
        'date_time': (2020, 11, 21, 10, 39, 46),
        'compress_type': 0,
        '_compresslevel': None,
        'comment': b'',
        'extra': b'UT\x05\x00\x03q\xee\xb8_ux\x0b\x00\x01\x04\xf4\x01\x00\x00\x04\xf4\x01\x00\x00',
        'create_system': 3,
        'create_version': 30,
        'extract_version': 10,
        'reserved': 0,
        'flag_bits': 0,
        'volume': 0,
        'internal_attr': 0,
        'external_attr': 2175008768,
        'header_offset': 5639544,
        'CRC': 764839713,
        'compress_size': 15190,
        'file_size': 15190,
        '_raw_time': 21751,
        'name': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/time_coordinates.nc',  # noqa
        'size': 15190,
        'type': 'file'},
    'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/trsp.nc': {
        'orig_filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/trsp.nc',  # noqa
        'filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/trsp.nc',  # noqa
        'date_time': (2020, 11, 21, 10, 39, 46),
        'compress_type': 0,
        '_compresslevel': None,
        'comment': b'',
        'extra': b'UT\x05\x00\x03q\xee\xb8_ux\x0b\x00\x01\x04\xf4\x01\x00\x00\x04\xf4\x01\x00\x00',
        'create_system': 3,
        'create_version': 30,
        'extract_version': 10,
        'reserved': 0,
        'flag_bits': 0,
        'volume': 0,
        'internal_attr': 0,
        'external_attr': 2175008768,
        'header_offset': 31746664,
        'CRC': 395113973,
        'compress_size': 336821,
        'file_size': 336821,
        '_raw_time': 21751,
        'name': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/trsp.nc',  # noqa
        'size': 336821,
        'type': 'file'},
    'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/tsm_nn.nc': {
        'orig_filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/tsm_nn.nc',  # noqa
        'filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/tsm_nn.nc',  # noqa
        'date_time': (2020, 11, 21, 10, 39, 46),
        'compress_type': 0,
        '_compresslevel': None,
        'comment': b'',
        'extra': b'UT\x05\x00\x03q\xee\xb8_ux\x0b\x00\x01\x04\xf4\x01\x00\x00\x04\xf4\x01\x00\x00',
        'create_system': 3,
        'create_version': 30,
        'extract_version': 10,
        'reserved': 0,
        'flag_bits': 0,
        'volume': 0,
        'internal_attr': 0,
        'external_attr': 2175008768,
        'header_offset': 25830416,
        'CRC': 1664095608,
        'compress_size': 418855,
        'file_size': 418855,
        '_raw_time': 21751,
        'name': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/tsm_nn.nc',  # noqa
        'size': 418855,
        'type': 'file'},
    'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/w_aer.nc': {
        'orig_filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/w_aer.nc',  # noqa
        'filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/w_aer.nc',  # noqa
        'date_time': (2020, 11, 21, 10, 39, 46),
        'compress_type': 0,
        '_compresslevel': None,
        'comment': b'',
        'extra': b'UT\x05\x00\x03q\xee\xb8_ux\x0b\x00\x01\x04\xf4\x01\x00\x00\x04\xf4\x01\x00\x00',
        'create_system': 3,
        'create_version': 30,
        'extract_version': 10,
        'reserved': 0,
        'flag_bits': 0,
        'volume': 0,
        'internal_attr': 0,
        'external_attr': 2175008768,
        'header_offset': 24130436,
        'CRC': 4151832868,
        'compress_size': 576321,
        'file_size': 576321,
        '_raw_time': 21751,
        'name': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/w_aer.nc',  # noqa
        'size': 576321,
        'type': 'file'},
    'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/wqsf.nc': {
        'orig_filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/wqsf.nc',  # noqa
        'filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/wqsf.nc',  # noqa
        'date_time': (2020, 11, 21, 10, 39, 46),
        'compress_type': 0,
        '_compresslevel': None,
        'comment': b'',
        'extra': b'UT\x05\x00\x03q\xee\xb8_ux\x0b\x00\x01\x04\xf4\x01\x00\x00\x04\xf4\x01\x00\x00',
        'create_system': 3,
        'create_version': 30,
        'extract_version': 10,
        'reserved': 0,
        'flag_bits': 0,
        'volume': 0,
        'internal_attr': 0,
        'external_attr': 2175008768,
        'header_offset': 34448951,
        'CRC': 2110029801,
        'compress_size': 2325556,
        'file_size': 2325556,
        '_raw_time': 21751,
        'name': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/wqsf.nc',  # noqa
        'size': 2325556,
        'type': 'file'},
    'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/xfdumanifest.xml': {  # noqa
        'orig_filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/xfdumanifest.xml',  # noqa
        'filename': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/xfdumanifest.xml',  # noqa
        'date_time': (2020, 11, 21, 10, 39, 52),
        'compress_type': 0,
        '_compresslevel': None,
        'comment': b'',
        'extra': b'UT\x05\x00\x03x\xee\xb8_ux\x0b\x00\x01\x04\xf4\x01\x00\x00\x04\xf4\x01\x00\x00',
        'create_system': 3,
        'create_version': 30,
        'extract_version': 10,
        'reserved': 0,
        'flag_bits': 0,
        'volume': 0,
        'internal_attr': 0,
        'external_attr': 2175008768,
        'header_offset': 36774672,
        'CRC': 1520601988,
        'compress_size': 192845,
        'file_size': 192845,
        '_raw_time': 21754,
        'name': 'S3A_OL_2_WFR____20201121T075933_20201121T080210_20201121T103050_0157_065_192_1980_MAR_O_NR_002.SEN3/xfdumanifest.xml',  # noqa
        'size': 192845,
        'type': 'file'}}


class TestLastFilesGetter(unittest.TestCase):
    """Test case for files getter."""

    def setUp(self):
        """Set up the test case."""
        from pytroll_collectors import s3stalker
        s3stalker.set_last_fetch(datetime.datetime(2000, 1, 1, 0, 0, tzinfo=tzutc()))
        self.ls_output = deepcopy(ls_output)

    @mock.patch('s3fs.S3FileSystem')
    def test_get_last_files_returns_files(self, s3_fs):
        """Test files are returned."""
        from pytroll_collectors import s3stalker
        path = "sentinel-s3-ol2wfr-zips/2020/11/21"
        s3_fs.return_value.ls.return_value = self.ls_output
        fs, files = s3stalker.get_last_files(path, anon=True)
        assert list(files) == self.ls_output

    @mock.patch('s3fs.S3FileSystem')
    def test_get_last_files_returns_incrementally(self, s3_fs):
        """Test files are newer than epoch."""
        from pytroll_collectors import s3stalker
        path = "sentinel-s3-ol2wfr-zips/2020/11/21"
        sorted_output = sorted(self.ls_output, key=(lambda x: x['LastModified']))
        s3_fs.return_value.ls.return_value = sorted_output[:8]

        fs, files = s3stalker.get_last_files(path, anon=True)
        assert len(files) == 8
        fetch_date = sorted_output[7]['LastModified']

        s3_fs.return_value.ls.return_value = self.ls_output
        fs, newer_files = s3stalker.get_last_files(path, anon=True)
        assert all(new_file['LastModified'] > fetch_date for new_file in newer_files)
        assert len(newer_files) == 8

    @mock.patch('s3fs.S3FileSystem')
    def test_get_last_files_returns_fs(self, s3_fs):
        """Test fs is returned."""
        from pytroll_collectors import s3stalker
        path = "sentinel-s3-ol2wfr-zips/2020/11/21"
        s3_fs.return_value.to_json.return_value = fs_json
        fs, files = s3stalker.get_last_files(path, anon=True)
        assert fs.to_json() == fs_json

    @mock.patch('s3fs.S3FileSystem')
    def test_get_last_files_filters_according_to_pattern(self, s3_fs):
        """Test fs is returned."""
        from pytroll_collectors import s3stalker
        path = "sentinel-s3-ol2wfr-zips/2020/11/21/"
        s3_fs.return_value.ls.return_value = self.ls_output
        fs, files = s3stalker.get_last_files(path, pattern=zip_pattern, anon=True)
        assert len(list(files)) == len(ls_output) / 2

    @mock.patch('s3fs.S3FileSystem')
    def test_get_last_files_with_pattern_add_metadata(self, s3_fs):
        """Test fs is returned."""
        from pytroll_collectors import s3stalker
        path = "sentinel-s3-ol2wfr-zips/2020/11/21/"
        s3_fs.return_value.ls.return_value = self.ls_output
        fs, files = s3stalker.get_last_files(path, pattern=zip_pattern, anon=True)
        assert 'metadata' in files[0]
        assert files[0]['metadata']['platform_name'] == 'S3A'


class TestFileListToMessages(unittest.TestCase):
    """Test case for filelist_to_messages."""

    def setUp(self):
        """Set up the test case."""
        from pytroll_collectors import s3stalker
        s3stalker.set_last_fetch(datetime.datetime(2000, 1, 1, 0, 0, tzinfo=tzutc()))

    @mock.patch('s3fs.S3FileSystem')
    def test_file_list_to_messages_returns_right_number_of_messages(self, s3_fs):
        """Test the right number of messages is returned."""
        from pytroll_collectors import s3stalker
        path = "sentinel-s3-ol2wfr-zips/2020/11/21"
        s3_fs.return_value.ls.return_value = ls_output
        s3_fs.return_value.to_json.return_value = fs_json
        fs, files = s3stalker.get_last_files(path, anon=True)
        message_list = pytroll_collectors.fsspec_to_message.filelist_to_messages(fs, files, subject)
        assert len(message_list) == len(files)

    @mock.patch('s3fs.S3FileSystem')
    def test_file_list_to_messages_returns_messages_containing_uris(self, s3_fs):
        """Test uris are in the messages."""
        from pytroll_collectors import s3stalker
        path = "sentinel-s3-ol2wfr-zips/2020/11/21"
        s3_fs.return_value.ls.return_value = ls_output
        s3_fs.return_value.to_json.return_value = fs_json
        fs, files = s3stalker.get_last_files(path, anon=True)
        message_list = pytroll_collectors.fsspec_to_message.filelist_to_messages(fs, files, subject)
        assert 'uri' in message_list[0].data


class TestFileListUnzipToMessages(unittest.TestCase):
    """Test filelist_unzip_to_messages."""

    def setUp(self):
        """Set up the test case."""
        from pytroll_collectors import s3stalker
        s3stalker.set_last_fetch(datetime.datetime(2000, 1, 1, 0, 0, tzinfo=tzutc()))

    @mock.patch('s3fs.S3FileSystem')
    @mock.patch('pytroll_collectors.fsspec_to_message.get_filesystem_class')
    def test_file_list_unzip_to_messages_returns_right_number_of_messages(self, zip_fs, s3_fs):
        """Test there are as many messages as files."""
        from pytroll_collectors import s3stalker
        path = "sentinel-s3-ol2wfr-zips/2020/11/21"
        s3_fs.return_value.ls.return_value = ls_output
        s3_fs.return_value.to_json.return_value = fs_json
        fs, files = s3stalker.get_last_files(path, anon=True)
        message_list = pytroll_collectors.fsspec_to_message.filelist_unzip_to_messages(fs, files, subject)
        assert len(message_list) == len(files)

    @mock.patch('s3fs.S3FileSystem')
    @mock.patch('pytroll_collectors.fsspec_to_message.get_filesystem_class')
    def test_file_list_unzip_to_messages_returns_messages_with_datasets_when_zip_file_is_source(self, zip_fs, s3_fs):
        """Test messages are of type dataset."""
        from pytroll_collectors import s3stalker
        path = "sentinel-s3-ol2wfr-zips/2020/11/21"
        s3_fs.return_value.ls.return_value = ls_output
        s3_fs.return_value.to_json.return_value = fs_json
        fs, files = s3stalker.get_last_files(path, anon=True)
        message_list = pytroll_collectors.fsspec_to_message.filelist_unzip_to_messages(fs, files, subject)
        assert 'dataset' in message_list[1].data

    @mock.patch('s3fs.S3FileSystem')
    @mock.patch('pytroll_collectors.fsspec_to_message.get_filesystem_class')
    def test_file_list_unzip_to_messages_returns_messages_with_right_amount_of_files(self, zip_fs, s3_fs):
        """Test right amount of files is present in messages."""
        from pytroll_collectors import s3stalker
        path = "sentinel-s3-ol2wfr-zips/2020/11/21"
        s3_fs.return_value.ls.return_value = ls_output
        s3_fs.return_value.to_json.return_value = fs_json
        zip_fs.return_value.return_value.find.return_value = zip_content
        zip_fs.return_value.return_value.to_json.return_value = zip_json
        fs, files = s3stalker.get_last_files(path, anon=True)
        message_list = pytroll_collectors.fsspec_to_message.filelist_unzip_to_messages(fs, files, subject)
        exp_file_list = [file['name'] for file in zip_content.values()]
        file_list = [file['uri'] for file in message_list[1].data['dataset']]
        assert len(file_list) == len(exp_file_list)

    @mock.patch('s3fs.S3FileSystem')
    @mock.patch('pytroll_collectors.fsspec_to_message.get_filesystem_class')
    def test_file_list_unzip_to_messages_returns_messages_with_list_of_zip_content_in_uid(self, zip_fs, s3_fs):
        """Test zip content is included in messages."""
        from pytroll_collectors import s3stalker
        path = "sentinel-s3-ol2wfr-zips/2020/11/21"
        s3_fs.return_value.ls.return_value = ls_output
        s3_fs.return_value.to_json.return_value = fs_json
        zip_fs.return_value.return_value.find.return_value = zip_content
        zip_fs.return_value.return_value.to_json.return_value = zip_json
        fs, files = s3stalker.get_last_files(path, anon=True)
        message_list = pytroll_collectors.fsspec_to_message.filelist_unzip_to_messages(fs, files, subject)
        exp_file_list = ['zip://' + file['name'] for file in zip_content.values()]
        file_list = [file['uid'] for file in message_list[1].data['dataset']]
        assert file_list == exp_file_list

    @mock.patch('s3fs.S3FileSystem')
    @mock.patch('pytroll_collectors.fsspec_to_message.get_filesystem_class')
    def test_file_list_unzip_to_messages_returns_messages_with_list_of_zip_content_in_uri(self, zip_fs, s3_fs):
        """Test zip content is included in messages."""
        from pytroll_collectors import s3stalker
        path = "sentinel-s3-ol2wfr-zips/2020/11/21"
        s3_fs.return_value.ls.return_value = ls_output
        s3_fs.return_value.to_json.return_value = fs_json
        zip_fs.return_value.return_value.find.return_value = zip_content
        zip_fs.return_value.return_value.to_json.return_value = zip_json
        fs, files = s3stalker.get_last_files(path, anon=True)
        message_list = pytroll_collectors.fsspec_to_message.filelist_unzip_to_messages(fs, files, subject)
        zip_file = ls_output[1]['name']
        exp_file_list = ['zip://' + file['name'] + '::s3://' + zip_file for file in zip_content.values()]
        file_list = [file['uri'] for file in message_list[1].data['dataset']]
        assert file_list == exp_file_list

    @mock.patch('s3fs.S3FileSystem')
    @mock.patch('pytroll_collectors.fsspec_to_message.get_filesystem_class')
    def test_file_list_unzip_to_messages_has_correct_subject(self, zip_fs, s3_fs):
        """Test filelist_unzip_to_messages has correct subject."""
        from pytroll_collectors import s3stalker
        path = "sentinel-s3-ol2wfr-zips/2020/11/21"
        s3_fs.return_value.ls.return_value = ls_output
        s3_fs.return_value.to_json.return_value = fs_json
        zip_fs.return_value.return_value.find.return_value = zip_content
        zip_fs.return_value.return_value.to_json.return_value = zip_json
        fs, files = s3stalker.get_last_files(path, anon=True)
        message_list = pytroll_collectors.fsspec_to_message.filelist_unzip_to_messages(fs, files, subject)
        assert message_list[1].subject == subject

    @mock.patch('s3fs.S3FileSystem')
    @mock.patch('pytroll_collectors.fsspec_to_message.get_filesystem_class')
    def test_file_list_unzip_to_messages_has_metadata(self, zip_fs, s3_fs):
        """Test filelist_unzip_to_messages has correct subject."""
        from pytroll_collectors import s3stalker
        path = "sentinel-s3-ol2wfr-zips/2020/11/21"
        s3_fs.return_value.ls.return_value = ls_output
        s3_fs.return_value.to_json.return_value = fs_json
        zip_fs.return_value.return_value.find.return_value = zip_content
        zip_fs.return_value.return_value.to_json.return_value = zip_json
        fs, files = s3stalker.get_last_files(path, pattern=zip_pattern, anon=True)
        message_list = pytroll_collectors.fsspec_to_message.filelist_unzip_to_messages(fs, files, subject)
        assert message_list[0].data['platform_name'] == 'S3A'


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


@mock.patch.object(pytroll_collectors.s3stalker.S3StalkerRunner, '_set_signal_shutdown')
def test_s3stalker_runner_ini(_set_signal_shutdown):
    """Test initialize/instanciate the S3StalkerRunner class."""
    _set_signal_shutdown.return_value = None

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


@mock.patch.object(pytroll_collectors.s3stalker.S3StalkerRunner, '_set_signal_shutdown')
def test_s3stalker_runner_get_timedelta(signal_shutdown):
    """Test getting the delta time defining the how far back in time to search for new files in the bucket."""
    signal_shutdown.return_value = None

    startup_timedelta_seconds = 2000
    bucket = 'atms-sdr'

    s3runner = S3StalkerRunner(bucket, S3_STALKER_CONFIG, startup_timedelta_seconds)

    last_fetch_time = None
    first_run = True
    result = s3runner._get_timedelta(last_fetch_time, is_first_run=first_run)

    assert result == {'seconds': 2000}


@mock.patch.object(pytroll_collectors.s3stalker.S3StalkerRunner, '_set_signal_shutdown')
@mock.patch.object(pytroll_collectors.s3stalker.S3StalkerRunner, '_process_messages')
@freeze_time('2022-12-20 10:10:0')
def test_do_fetch_most_recent(process_messages, signal_shutdown):
    """Test getting the time of the last files fetch."""
    signal_shutdown.return_value = None
    process_messages.return_value = None

    startup_timedelta_seconds = 3600
    bucket = 'atms-sdr'

    s3runner = S3StalkerRunner(bucket, S3_STALKER_CONFIG, startup_timedelta_seconds)

    set_last_fetch(datetime.datetime.now(tzone.UTC) - datetime.timedelta(seconds=startup_timedelta_seconds))

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

    def __init__(self, dummy):
        """Initialize the fake publisher class."""
        return

    def send(self, msg):
        """Faking the sending of a message."""
        return msg

    def __call__(self, msg):
        """Faking a call method."""
        return self.send(msg)


class TestS3StalkerRunner:
    """Test the S3 Stalker Runner functionalities."""

    def setup_method(self):
        """Set up the test case."""
        # s3stalker.set_last_fetch(datetime.datetime(2000, 1, 1, 0, 0, tzinfo=tzutc()))
        self.ls_output = deepcopy(ATMS_FILES)
        start_time = datetime.datetime(2022, 12, 20, 12, 0)
        now = datetime.datetime.utcnow()
        self.delta_sec = (now - start_time).total_seconds()
        self.bucket = 'atms-sdr'
        self.config = S3_STALKER_CONFIG

    @mock.patch('s3fs.S3FileSystem')
    @mock.patch.object(pytroll_collectors.s3stalker.S3StalkerRunner, '_set_signal_shutdown')
    def test_create_messages_for_recent_files(self, signal_shutdown, s3_fs):
        """Test create messages for recent files arriving in the bucket."""
        signal_shutdown.return_value = None
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

    @mock.patch.object(pytroll_collectors.s3stalker.S3StalkerRunner, '_set_signal_shutdown')
    @freeze_time('2022-12-20 10:10:0')
    def test_get_seconds_back_to_search(self, signal_shutdown):
        """Test get seconds back in time to search for new files."""
        signal_shutdown.return_value = None
        startup_timedelta_seconds = 2000

        s3runner = S3StalkerRunner(self.bucket, S3_STALKER_CONFIG, startup_timedelta_seconds)

        s3runner._timedelta = {'seconds': self.delta_sec}

        result = s3runner._get_seconds_back_to_search(None)
        assert result == 120.0

        set_last_fetch(datetime.datetime.now(tzone.UTC) - datetime.timedelta(seconds=startup_timedelta_seconds))
        last_fetch_time = get_last_fetch()
        result = s3runner._get_seconds_back_to_search(last_fetch_time)
        assert result == 2000.0

    @mock.patch('s3fs.S3FileSystem')
    @mock.patch.object(pytroll_collectors.s3stalker.S3StalkerRunner, '_set_signal_shutdown')
    def test_process_messages(self, signal_shutdown, s3_fs, caplog):
        """Test process the messages."""
        signal_shutdown.return_value = None
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
        assert "waiting time:" in res[2]
