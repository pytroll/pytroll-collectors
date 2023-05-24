#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2018 - 2021 Pytroll developers
#
# Author(s):
#
#   Ruben Sala <rsala@tecnavia.com>
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

import tempfile
import os

from configparser import RawConfigParser
from collections import OrderedDict

from bin.trollstalker import check_if_dir_exist, create_notifier


class TestTrollStalker:

    def setup_method(self):
        self.config_item = "hrit"
        self.config = RawConfigParser()
        self.config.read('data/trollstalker_config.ini')
        self.config = OrderedDict(self.config.items(self.config_item))

    def test_monitored_dir_exist(self):

        temp_dir = tempfile.TemporaryDirectory()
        monitored_dirs = [os.path.join(temp_dir.name, "folder_1")]

        notifier = create_notifier(self.config['topic'],
                                   self.config['instruments'],
                                   self.config['posttroll_port'],
                                   self.config['filepattern'],
                                   self.config['event_names'],
                                   monitored_dirs,
                                   self.config_item)

        for monitored_dir in monitored_dirs:
            assert os.path.exists(monitored_dir)

        notifier.start()
        notifier.stop()
