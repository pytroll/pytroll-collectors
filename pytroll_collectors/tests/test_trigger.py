#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014 Martin Raspaud

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

"""Unittests for triggers."""

import time
import unittest
from datetime import datetime, timedelta

from unittest.mock import patch, Mock, call

messages = ['']


class FakeMessage(object):
    """Fake messages."""

    def __init__(self, data, type='file'):
        """Init the fake message."""
        self.data = data
        self.type = type


class TestPostTrollTrigger(unittest.TestCase):
    """Test the posttroll trigger."""

    @patch('pytroll_collectors.trigger.NSSubscriber')
    def test_timeout(self, nssub):
        """Test timing out."""
        from pytroll_collectors.trigger import PostTrollTrigger
        collector = Mock()
        collector.timeout = datetime.utcnow() + timedelta(seconds=.2)
        collector.return_value = None

        def terminator(obj, publish_topic=None):
            collector.timeout = None
        ptt = PostTrollTrigger([collector], terminator, None, None,
                               publish_topic=None)

        sub = ptt.msgproc.nssub.start.return_value
        sub.recv.return_value = iter([FakeMessage({"a": "a", 'start_time': 1, 'end_time': 2}),
                                      FakeMessage(
                                          {"b": "b", 'start_time': 1, 'end_time': 2}),
                                      FakeMessage({"c": "c", 'start_time': 1, 'end_time': 2})])

        ptt.start()
        time.sleep(.4)
        ptt.stop()
        self.assertTrue(collector.timeout is None)

    def test_duration(self):
        """Test duration"""
        from pytroll_collectors.trigger import PostTrollTrigger
        ptt = PostTrollTrigger(None, None, None, None, duration=60)

        msg_data = ptt.decode_message(FakeMessage({"a": "a", 'start_time': datetime(2020, 1, 21, 11, 27)}))

        self.assertIn("end_time", msg_data)
        self.assertEqual(msg_data["end_time"], datetime(2020, 1, 21, 11, 28))


class TestAbstractMessageProcessor(unittest.TestCase):
    """Test AbstractMessageProcessor."""

    @patch('pytroll_collectors.trigger.AbstractMessageProcessor.process')
    @patch('pytroll_collectors.trigger.Thread')
    @patch('pytroll_collectors.trigger.NSSubscriber')
    def test_all(self, NSSubscriber, Thread, process):
        """Test the run() method."""
        from pytroll_collectors.trigger import AbstractMessageProcessor

        msg_file = Mock(type='file')
        msg_collection = Mock(type='collection')
        msg_dataset = Mock(type='dataset')
        msg_foo = Mock(type='foo')
        recv = Mock()
        recv.return_value = [None, msg_file, msg_collection, msg_dataset,
                             msg_foo]
        sub = Mock()
        sub.recv = recv
        NSSubscriber.return_value.start.return_value = sub

        proc = AbstractMessageProcessor('foo', 'bar', nameserver='baz')
        NSSubscriber.assert_called_with('foo', 'bar', True,
                                        nameserver='baz')
        proc.start()
        Thread.start.assert_called()
        proc.run()
        assert call(msg_file) in process.mock_calls
        assert call(msg_collection) in process.mock_calls
        assert call(msg_dataset) in process.mock_calls
        assert call(msg_foo) not in process.mock_calls
        assert call(None) not in process.mock_calls

        proc.nssub.stop.assert_called()
        assert proc.loop is False


def suite():
    """Test suite for test_trigger."""
    loader = unittest.TestLoader()
    mysuite = unittest.TestSuite()
    mysuite.addTest(loader.loadTestsFromTestCase(TestPostTrollTrigger))

    return mysuite


if __name__ == '__main__':
    unittest.main()
