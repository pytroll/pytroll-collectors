#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2022 Pytroll developers
#
# Author(s):
#
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

"""Utility functions."""

from posttroll.publisher import create_publisher_from_dict_config


def check_nameserver_options(nameservers, for_listener=False):
    """Check the nameserver options given by user."""
    if for_listener:
        if isinstance(nameservers, (tuple, list)):
            nameservers = nameservers[0]
    if nameservers is False:
        return nameservers
    if nameservers is not None:
        if isinstance(nameservers, str):
            nameservers = False if nameservers == 'false' else nameservers
        else:
            nameservers = False if 'false' in nameservers or False in nameservers else nameservers
    return nameservers


def create_started_publisher_from_config(publisher_config):
    """Create a started publisher from a dictionary of configuration items."""
    publisher = create_publisher_from_dict_config(publisher_config)
    publisher.start()
    return publisher


def create_publisher_config_dict(name, nameservers, port):
    """Create pubisher configuration dictionary from the given name, port and nameserver."""
    return {
        'name': name,
        'port': port,
        'nameservers': nameservers,
    }
