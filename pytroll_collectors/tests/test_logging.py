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

import yaml
from posttroll.message import Message
from posttroll.testing import patched_publisher
from upath import UPath


# pytroll_collectors/tests/test_logging.py

import logging

import pytest
import yaml

from pytroll_collectors.logging import _setup_logging_from_config


@pytest.fixture(autouse=True)
def clean_root_logger():
    """Avoid logging state leaking between tests."""
    root = logging.getLogger()

    old_handlers = root.handlers[:]
    old_level = root.level

    # Remove current handlers
    for handler in root.handlers[:]:
        root.removeHandler(handler)
        handler.close()

    yield

    # Clean handlers added during test
    for handler in root.handlers[:]:
        root.removeHandler(handler)
        handler.close()

    # Restore original handlers
    for handler in old_handlers:
        root.addHandler(handler)

    root.setLevel(old_level)


def test_setup_logging_from_yaml_config(tmp_path):
    """Test setting up logging from a YAML dictConfig file."""
    log_config_file = tmp_path / "log_config.yaml"
    handler_name = "console123"

    log_config = {
        "version": 1,
        "handlers": {
            handler_name: {
                "class": "logging.StreamHandler",
                "level": "INFO",
            },
        },
        "loggers": {
            "": {
                "level": "INFO",
                "handlers": [handler_name],
            },
        },
    }

    log_config_file.write_text(yaml.dump(log_config), encoding="utf-8")

    logger = _setup_logging_from_config(log_config_file, "pytroll_collectors.test")

    root = logging.getLogger()
    assert logger.name == "pytroll_collectors.test"
    assert len(root.handlers) == 1
    assert root.handlers[0].name == handler_name
    assert root.level == logging.INFO


def test_setup_logging_from_missing_config_raises(tmp_path):
    """Test that a missing logging config file raises FileNotFoundError."""
    log_config_file = tmp_path / "missing.yaml"

    with pytest.raises(FileNotFoundError, match="Logging config file not found"):
        _setup_logging_from_config(log_config_file, "pytroll_collectors.test")


def test_setup_logging_from_invalid_yaml_raises(tmp_path):
    """Test that a YAML file not containing a dict raises ValueError."""
    log_config_file = tmp_path / "log_config.yaml"
    log_config_file.write_text("- not\n- a\n- dict\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Invalid YAML logging config"):
        _setup_logging_from_config(log_config_file, "pytroll_collectors.test")
