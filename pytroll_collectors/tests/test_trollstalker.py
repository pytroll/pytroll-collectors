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

import pytest

from bin.trollstalker import check_if_monitored_dir_exist

class TestTrollStalkerFunctions:

    def test_monitored_dir_init():
        assert check_if_monitored_dir_exist('data/monitored_dir_1') == True
        assert check_if_monitored_dir_exist('data/monitored_dir_2') == True

        # TEST invalids paths
        assert check_if_monitored_dir_exist('data/monitored_d:ir_3') == False
        assert check_if_monitored_dir_exist('data/monitored_d*ir_3') == False
        assert check_if_monitored_dir_exist('data/monitored_d?ir_3') == False
        assert check_if_monitored_dir_exist('data/monitored_d<ir_3') == False
        assert check_if_monitored_dir_exist('data/monitored_d>ir_3') == False
        assert check_if_monitored_dir_exist('data/monitored_d|ir_3') == False
