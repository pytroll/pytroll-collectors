#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2026 Pytroll developers

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

"""Unit testing the logging functionality.

This is inspired by how it is done in pytroll-watchers and assisted by ChatGPT.
"""


import pytest
import logging
import logging.config

import yaml
from unittest.mock import patch
from pytroll_collectors.logging import _setup_logging_from_config


@pytest.fixture
def isolated_logging():
    root = logging.getLogger()
    old_handlers = root.handlers[:]
    old_level = root.level

    for handler in root.handlers[:]:
        root.removeHandler(handler)

    yield

    for handler in root.handlers[:]:
        root.removeHandler(handler)
        handler.close()

    for handler in old_handlers:
        root.addHandler(handler)

    root.setLevel(old_level)


def test_setup_logging_from_yaml_config(tmp_path):
    config_file = tmp_path / "logging.yaml"
    config_file.write_text(
        """
version: 1
handlers:
  console:
    class: logging.StreamHandler
loggers:
  "":
    handlers: [console]
    level: INFO
""",
        encoding="utf-8",
    )

    with patch("pytroll_collectors.logging.logging.config.dictConfig") as dict_config:
        logger = _setup_logging_from_config(config_file, "mylogger")

    dict_config.assert_called_once()
    assert logger.name == "mylogger"


def test_setup_logging_from_missing_config_raises(tmp_path, isolated_logging):
    """Test that a missing logging config file raises FileNotFoundError."""
    log_config_file = tmp_path / "missing.yaml"

    with pytest.raises(FileNotFoundError, match="Logging config file not found"):
        _setup_logging_from_config(log_config_file, "pytroll_collectors.test")


def test_setup_logging_from_invalid_yaml_raises(tmp_path, isolated_logging):
    """Test that a YAML file not containing a dict raises ValueError."""
    log_config_file = tmp_path / "log_config.yaml"
    log_config_file.write_text("- not\n- a\n- dict\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Invalid YAML logging config"):
        _setup_logging_from_config(log_config_file, "pytroll_collectors.test")
