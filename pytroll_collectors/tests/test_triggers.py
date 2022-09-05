#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2014 - 2021 Pytroll developers
#
# Author(s):
#
#   Martin Raspaud <martin.raspaud@smhi.se>
#   Panu Lahtinen <panu.lahtinen@fmi.fi>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Unittests for triggers."""

import time
from datetime import datetime, timedelta

from unittest.mock import patch, Mock, call
import pytest


class FakeMessage(object):
    """Fake messages."""

    def __init__(self, data, msg_type='file'):
        """Init the fake message."""
        self.data = data
        self.type = msg_type


class TestPostTrollTrigger:
    """Test the posttroll trigger."""

    @patch('pytroll_collectors.triggers.PostTrollTrigger._get_metadata')
    @patch('pytroll_collectors.triggers._posttroll.create_subscriber_from_dict_config')
    def test_timeout(self, nssub, get_metadata):
        """Test timing out."""
        from pytroll_collectors.triggers import PostTrollTrigger

        messages = [FakeMessage({"a": "a", 'start_time': 1, 'end_time': 2, 'collection_area_id': 'area_id',
                                 'format': 'fmt', 'data_processing_level': 'l1b', 'uri': 'uri1'}),
                    FakeMessage({"b": "b", 'start_time': 2, 'end_time': 3, 'collection_area_id': 'area_id',
                                 'format': 'fmt', 'data_processing_level': 'l1b', 'uri': 'uri2'}),
                    FakeMessage({"c": "c", 'start_time': 3, 'end_time': 4, 'collection_area_id': 'area_id',
                                 'format': 'fmt', 'data_processing_level': 'l1b', 'uri': 'uri3'})]

        collector = Mock()
        collector.timeout = datetime.utcnow() + timedelta(seconds=.2)
        collector.return_value = None

        def finish():
            collector.timeout = None
            return [msg.data for msg in messages]
        collector.finish = finish
        publisher = Mock()

        ptt = PostTrollTrigger([collector], None, None, publisher,
                               publish_topic=None)
        sub = ptt.msgproc.subscriber.start.return_value
        sub.recv.return_value = iter(messages)
        ptt.start()
        time.sleep(.4)
        ptt.stop()

        # Timeout means a message should've been published
        publisher.send.assert_called_once()

    def test_duration(self):
        """Test duration."""
        from pytroll_collectors.triggers import PostTrollTrigger
        publisher = Mock()
        ptt = PostTrollTrigger(None, None, None, publisher, duration=60)

        msg_data = ptt._get_metadata(FakeMessage({"a": "a", 'start_time': datetime(2020, 1, 21, 11, 27)}))

        assert "end_time" in msg_data
        assert msg_data["end_time"] == datetime(2020, 1, 21, 11, 28)

    @patch('pytroll_collectors.triggers._posttroll.create_subscriber_from_dict_config')
    def test_inbound_connection_is_used(self, nssub):
        """Test that inbound_connection is used."""
        from pytroll_collectors.triggers import PostTrollTrigger
        publisher = Mock()
        host_info = ["myhost:9999"]
        PostTrollTrigger(None, None, None, publisher, inbound_connection=host_info)
        passed_settings = nssub.mock_calls[0].args[0]
        assert passed_settings["addresses"] == ["tcp://" + host_info[0]]

    @patch('pytroll_collectors.triggers._posttroll.create_subscriber_from_dict_config')
    def test_subscribe_nameserver_defaults_to_localhost(self, nssub):
        """Test that inbound_connection is used."""
        from pytroll_collectors.triggers import PostTrollTrigger
        publisher = Mock()
        PostTrollTrigger(None, None, None, publisher, nameserver=None)
        passed_settings = nssub.mock_calls[0].args[0]
        assert passed_settings["nameserver"] == "localhost"

    @patch('pytroll_collectors.triggers._posttroll.create_subscriber_from_dict_config')
    def test_both_inbound_connection_and_nameserver_are_used(self, nssub):
        """Test that inbound_connection is used over nameserver."""
        from pytroll_collectors.triggers import PostTrollTrigger
        publisher = Mock()
        host_info = ["myhost:9999"]
        nameserver = "someotherhost"
        PostTrollTrigger(None, None, None, publisher, inbound_connection=host_info, nameserver=nameserver)
        passed_settings = nssub.mock_calls[0].args[0]
        assert passed_settings["addresses"] == ["tcp://" + host_info[0]]
        assert passed_settings["nameserver"] == nameserver

    @patch('pytroll_collectors.triggers._posttroll.create_subscriber_from_dict_config')
    def test_nameserver_defaults_to_false_when_inbound_connection_is_passed(self, nssub):
        """Test that nameserver defaults to false when inbound_connection is passed."""
        from pytroll_collectors.triggers import PostTrollTrigger
        publisher = Mock()
        host_info = ["myhost:9999"]
        nameserver = None
        PostTrollTrigger(None, None, None, publisher, inbound_connection=host_info, nameserver=nameserver)
        passed_settings = nssub.mock_calls[0].args[0]
        assert passed_settings["addresses"] == ["tcp://" + host_info[0]]
        assert passed_settings["nameserver"] is False

    @patch('pytroll_collectors.triggers._posttroll.create_subscriber_from_dict_config')
    def test_inbound_connection_can_replace_nameserver(self, nssub):
        """Test that inbound_connection can replace nameserver."""
        from pytroll_collectors.triggers import PostTrollTrigger
        publisher = Mock()
        host_info = ["mynameserverhost"]
        nameserver = None
        PostTrollTrigger(None, None, None, publisher, inbound_connection=host_info, nameserver=nameserver)
        passed_settings = nssub.mock_calls[0].args[0]
        assert not passed_settings.get("addresses")
        assert passed_settings["nameserver"] == host_info[0]

    @patch('pytroll_collectors.triggers._posttroll.create_subscriber_from_dict_config')
    def test_inbound_connection_be_split(self, nssub):
        """Test that inbound_connection can be split into addresses and nameserver."""
        from pytroll_collectors.triggers import PostTrollTrigger
        publisher = Mock()
        host_info = ["myhost:9999"]
        nameserver_info = "mynameserverhost"
        nameserver = None
        inbound_connection = host_info + [nameserver_info]
        PostTrollTrigger(None, None, None, publisher,
                         inbound_connection=inbound_connection, nameserver=nameserver)
        passed_settings = nssub.mock_calls[0].args[0]
        assert passed_settings["addresses"] == ["tcp://" + host_info[0]]
        assert passed_settings["nameserver"] == nameserver_info

    @patch('pytroll_collectors.triggers._posttroll.create_subscriber_from_dict_config')
    def test_inbound_connection_can_only_contain_one_nameserver(self, nssub):
        """Test that inbound_connection can only contain one nameserver."""
        from pytroll_collectors.triggers import PostTrollTrigger
        publisher = Mock()
        nameserver_info = ["mynameserverhost", "myothernameserverhost"]
        inbound_connection = nameserver_info
        with pytest.raises(ValueError):
            PostTrollTrigger(None, None, None, publisher, inbound_connection=inbound_connection)

    @patch('pytroll_collectors.triggers._posttroll.create_subscriber_from_dict_config')
    def test_passing_nameserver_issues_a_deprecation_warning(self, nssub):
        """Test that passing nameserver issues a deprecation warning."""
        from pytroll_collectors.triggers import PostTrollTrigger
        publisher = Mock()
        nameserver = "mynameserver"
        with pytest.deprecated_call():
            PostTrollTrigger(None, None, None, publisher, nameserver=nameserver)


class TestMessageProcessor:
    """Test _MessageProcessor."""

    @patch('pytroll_collectors.triggers._posttroll._MessageProcessor.process')
    @patch('pytroll_collectors.triggers._posttroll.Thread')
    @patch('pytroll_collectors.triggers._posttroll.create_subscriber_from_dict_config')
    def test_all(self, sub_factory, Thread, process):
        """Test the run() method."""
        from pytroll_collectors.triggers._posttroll import _MessageProcessor

        msg_file = Mock(type='file')
        msg_collection = Mock(type='collection')
        msg_dataset = Mock(type='dataset')
        msg_foo = Mock(type='foo')
        recv = Mock()
        recv.return_value = [None, msg_file, msg_collection, msg_dataset,
                             msg_foo]
        sub = Mock()
        sub.recv = recv
        sub_factory.return_value = sub

        proc = _MessageProcessor('foo', 'bar', nameserver='baz')
        expected_config = dict(services="foo", topics="bar", nameserver="baz", addr_listener=True)
        sub_factory.assert_called_with(expected_config)
        proc.start()
        Thread.start.assert_called()
        proc.run()
        assert call(msg_file) in process.mock_calls
        assert call(msg_collection) in process.mock_calls
        assert call(msg_dataset) in process.mock_calls
        assert call(msg_foo) not in process.mock_calls
        assert call(None) not in process.mock_calls

        proc.subscriber.stop.assert_called()
        assert proc.loop is False


class TestFileTrigger:
    """Test the FileTrigger class."""

    def test_getting_metadata(self):
        """Test getting the metadata."""
        from pytroll_collectors.triggers._base import FileTrigger
        import configparser
        collectors = []
        config = configparser.ConfigParser(interpolation=None)
        section = "section1"
        config.add_section(section)
        config.set(section, "pattern", "{name}_{start_time:%Y%m%dT%H%M}_{end_time:%Y%m%dT%H%M}.data")
        publisher = None
        trigger = FileTrigger(collectors, dict(config.items("section1")), publisher, publish_topic=None,
                              publish_message_after_each_reception=False)
        res = trigger._get_metadata("somefile_20220512T1544_20220512T1545.data")

        assert res == {"name": "somefile",
                       'end_time': datetime(2022, 5, 12, 15, 45),
                       'filename': 'somefile_20220512T1544_20220512T1545.data',
                       'start_time': datetime(2022, 5, 12, 15, 44),
                       'uri': 'somefile_20220512T1544_20220512T1545.data'
                       }

    def test_getting_metadata_does_not_involve_multiple_sections(self):
        """Test that getting metadata does not involve multiple sections."""
        from pytroll_collectors.triggers._base import FileTrigger
        import configparser
        collectors = []
        config = configparser.ConfigParser(interpolation=None)
        section = "section1"
        config.add_section(section)
        config.set(section, "pattern", "{name}_{start_time:%Y%m%dT%H%M}_{end_time:%Y%m%dT%H%M}.data")
        config.set(section, "key1", "value1")
        section = "section2"
        config.add_section(section)
        config.set(section, "pattern", "{name}_{start_time:%Y%m%dT%H%M}_{end_time:%Y%m%dT%H%M}.data")
        config.set(section, "key2", "value2")
        publisher = None
        trigger = FileTrigger(collectors, dict(config.items("section1")), publisher, publish_topic=None,
                              publish_message_after_each_reception=False)
        res = trigger._get_metadata("somefile_20220512T1544_20220512T1545.data")

        assert res == {"name": "somefile",
                       'end_time': datetime(2022, 5, 12, 15, 45),
                       'filename': 'somefile_20220512T1544_20220512T1545.data',
                       'start_time': datetime(2022, 5, 12, 15, 44),
                       'uri': 'somefile_20220512T1544_20220512T1545.data',
                       'key1': "value1"
                       }

    def test_filetrigger_exception(self, caplog):
        """Test getting the metadata."""
        from pytroll_collectors.triggers._base import FileTrigger
        import configparser

        def _collectors(metadata):
            raise KeyError("Found no TLE entry for 'METOP-B' to simulate")
        collectors = [_collectors]
        config = configparser.ConfigParser(interpolation=None)
        section = "section1"
        config.add_section(section)
        config.set(section, "pattern", "{name}_{start_time:%Y%m%dT%H%M}_{end_time:%Y%m%dT%H%M}.data")
        publisher = None
        trigger = FileTrigger(collectors, dict(config.items("section1")), publisher, publish_topic=None,
                              publish_message_after_each_reception=False)
        trigger._process_metadata({'sensor': 'avhrr', 'platform_name': 'Metop-B',
                                   'start_time': datetime(2022, 9, 1, 10, 22, 3),
                                   'orbit_number': '51653',
                                   'uri': 'AVHRR_C_EUMP_20220901102203_51653_eps_o_amv_l2d.bin',
                                   'uid': 'AVHRR_C_EUMP_20220901102203_51653_eps_o_amv_l2d.bin',
                                   'origin': '157.249.16.188:9062',
                                   'end_time': datetime(2022, 9, 1, 10, 25, 3)})
        assert "Found no TLE entry for 'METOP-B' to simulate" in caplog.text

    @patch('pytroll_collectors.triggers._base.Trigger.publish_collection')
    def test_filetrigger_collecting_complete(self, patch_publish_collection):
        """Test getting the metadata."""
        from pytroll_collectors.triggers._base import FileTrigger
        import configparser
        granule_metadata = {'sensor': 'avhrr'}

        def _collectors(metadata):
            return True
        collectors = [_collectors]
        config = configparser.ConfigParser(interpolation=None)
        section = "section1"
        config.add_section(section)
        config.set(section, "pattern", "{name}_{start_time:%Y%m%dT%H%M}_{end_time:%Y%m%dT%H%M}.data")
        publisher = None
        trigger = FileTrigger(collectors, dict(config.items("section1")), publisher, publish_topic=None,
                              publish_message_after_each_reception=False)
        trigger._process_metadata(granule_metadata)

        patch_publish_collection.assert_called_once()
