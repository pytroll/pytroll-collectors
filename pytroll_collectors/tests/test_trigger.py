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

try:
    from unittest.mock import patch, Mock
except ImportError:
    from mock import patch, Mock

messages = ['']


class FakeMessage(object):
    """Fake messages."""

    def __init__(self, data):
        """Init the fake message."""
        self.data = data


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


def suite():
    """Test suite for test_trigger."""
    loader = unittest.TestLoader()
    mysuite = unittest.TestSuite()
    mysuite.addTest(loader.loadTestsFromTestCase(TestPostTrollTrigger))

    return mysuite


if __name__ == '__main__':
    unittest.main()
