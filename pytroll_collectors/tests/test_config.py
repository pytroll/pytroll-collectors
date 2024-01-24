#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <a000680@c21856.ad.smhi.se>

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

"""Test getting the yaml configurations from file."""

from pytroll_collectors.config import read_config


def test_get_yaml_configuration(fake_yamlconfig_file_for_scisys_receiver):
    """Test read and get the yaml configuration for the scisys receiver from file."""
    config = read_config(fake_yamlconfig_file_for_scisys_receiver)

    assert config['publish_topic_pattern'] == '/{sensor}/{format}/{data_processing_level}/{platform_name}'
    assert config['topic_postfix'] == 'my/cool/postfix/topic'
    assert config['host'] == 'merlin'
    assert isinstance(config['port'], int)
    assert config['port'] == 10600
    assert config['station'] == 'nrk'
    assert config['environment'] == 'dev'
    assert len(config['excluded_satellites']) == 1
    assert config['excluded_satellites'][0] == 'fy3d'
