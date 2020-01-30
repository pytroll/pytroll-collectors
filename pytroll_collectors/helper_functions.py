# -*- coding: utf-8 -*-
#
# Copyright (c) 2014-2018
#
# Author(s):
#
#   Panu Lahtinen <panu.lahtinen@fmi.fi>
#   Martin Raspaud <martin.raspaud@smhi.se>
#   Adam Dybbroe <adam.dybbroe@smhi.se>
#
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

"""Helper functions."""

import os
import datetime as dt
import re
import logging
from six.moves.urllib.parse import urlparse
import netifaces
import socket

from trollsift import compose

LOG = logging.getLogger(__name__)


def create_aligned_datetime_var(var_pattern, info_dict):
    """Create an aligned datetime variable.

    Uses *var_patterns* like "{time:%Y%m%d%H%M|align(15)}"
    to new datetime including support for temporal
    alignment (Ceil/Round a datetime object to a multiple of a timedelta.
    Useful to equalize small time differences in name of files
    belonging to the same timeslot).
    """
    mtch = re.match(
        '{(.*?)(!(.*?))?(\\:(.*?))?(\\|(.*?))?}',
        var_pattern)

    if mtch is None:
        return None

    # parse date format pattern
    key = mtch.groups()[0]
    # format_spec = mtch.groups()[4]
    transform = mtch.groups()[6]
    date_val = info_dict[key]

    if not isinstance(date_val, dt.datetime):
        return None

    # only for datetime types
    res = date_val
    if transform:
        align_params = _parse_align_time_transform(transform)
        if align_params:
            res = align_time(
                date_val,
                dt.timedelta(minutes=align_params[0]),
                dt.timedelta(minutes=align_params[1]),
                align_params[2])

    if res is None:
        # fallback to default compose when no special handling needed
        # NOTE: This will fail, there's no `var_val`!!!
        res = compose(var_val, self.info)  # noqa

    return res


def _parse_align_time_transform(transform_spec):
    """Parse the align-time transformation string "align(15,0,-1)" and returns *(steps, offset, intv_add)*."""
    match = re.search('align\\((.*)\\)', transform_spec)
    if match:
        al_args = match.group(1).split(',')
        steps = int(al_args[0])
        if len(al_args) > 1:
            offset = int(al_args[1])
        else:
            offset = 0
        if len(al_args) > 2:
            intv_add = int(al_args[2])
        else:
            intv_add = 0
        return (steps, offset, intv_add)
    else:
        return None


def align_time(input_val, steps=None,
               offset=None, intervals_to_add=0):
    """Ceil/Round a datetime object to a multiple of a timedelta.

    Useful to equalize small time differences in name of files
    belonging to the same timeslot
    """
    offset = offset or dt.timedelta(minutes=0)
    steps = steps or dt.timedelta(minutes=5)

    try:
        stepss = steps.total_seconds()
    # Python 2.6 compatibility hack
    except AttributeError:
        stepss = steps.days * 86400. + \
            steps.seconds + steps.microseconds * 1e-6
    val = input_val - offset
    vals = (val - val.min).seconds
    result = val - dt.timedelta(seconds=(vals - (vals // stepss) * stepss))
    result = result + (intervals_to_add * steps)
    return result


def parse_aliases(config):
    """Parse aliases from the config.

    Aliases are given in the config as:

    {'alias_<name>': 'value:alias'}, or
    {'alias_<name>': 'value1:alias1|value2:alias2'},

    where <name> is the name of the key which value will be
    replaced. The later form is there to support several possible
    substitutions (eg. '2' -> '9' and '3' -> '10' in the case of MSG).

    """
    aliases = {}

    for key in config:
        if 'alias' in key:
            alias = config[key]
            new_key = key.replace('alias_', '')
            if '|' in alias or ':' in alias:
                parts = alias.split('|')
                aliases2 = {}
                for part in parts:
                    key2, val2 = part.split(':')
                    aliases2[key2] = val2
                alias = aliases2
            aliases[new_key] = alias
    return aliases


def get_local_ips():
    """Get the local ips."""
    inet_addrs = [netifaces.ifaddresses(iface).get(netifaces.AF_INET)
                  for iface in netifaces.interfaces()]
    ips = []
    for addr in inet_addrs:
        if addr is not None:
            for add in addr:
                ips.append(add['addr'])
    return ips


def is_uri_on_server(uri, strict=False):
    """Check if the *uri* is designating a place on the server.

    If *strict* is True, the hostname has to be specified in the *uri*
    for the path to be considered valid.
    """
    url = urlparse(uri)
    LOG.debug("URL: %s", str(url))
    try:
        url_ip = socket.gethostbyname(url.hostname)
        LOG.debug("url_ip: %s", url_ip)
    except (socket.gaierror, TypeError):
        if strict:
            return False
        try:
            os.stat(url.path)
        except OSError:
            return False
    else:
        if url.hostname == '':
            if strict:
                return False
            try:
                os.stat(url.path)
            except OSError:
                return False
        elif url_ip not in get_local_ips():
            return False
        else:
            try:
                os.stat(url.path)
            except OSError:
                return False
    return True


def read_yaml(fname):
    """Read YAML file."""
    import yaml

    with open(fname, 'r') as fid:
        data = yaml.load(fid, Loader=yaml.SafeLoader)

    return data
