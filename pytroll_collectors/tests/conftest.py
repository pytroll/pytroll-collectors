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

"""Fixtures for unittests."""

import pytest


TEST_YAML_CONFIG_CONTENT_SCISYS_RECEIVER = """
# Publish topic
publish_topic_pattern: '/{sensor}/{format}/{data_processing_level}/{platform_name}'

# It is possible to here add a static postfix topic if needed:
topic_postfix: "my/cool/postfix/topic"

host: merlin
port: 10600
station: nrk
environment: dev

excluded_satellites:
  - fy3d

"""


@pytest.fixture
def fake_yamlconfig_file_for_scisys_receiver(tmp_path):
    """Write fake yaml config file for the SCISYS receiver."""
    file_path = tmp_path / 'test_scisys_receiver_config.yaml'
    with open(file_path, 'w') as fpt:
        fpt.write(TEST_YAML_CONFIG_CONTENT_SCISYS_RECEIVER)

    yield file_path
