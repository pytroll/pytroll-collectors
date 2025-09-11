#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2014 - 2021 Pytroll developers
#
# Author(s):
#   Martin Raspaud <martin.raspaud@smhi.se>
#   Panu Lahtinen <panu.lahtinen@fmi.fi>
#   Adam Dybbroe <adam.dybbroe@smhi.se>
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

"""Gather messages of granules that cover geographic regions together and send them as a collection."""

import sys
import time
import os
import os.path

from pytroll_collectors.geographic_gatherer import GeographicGatherer, arg_parse
from pytroll_collectors.logging import setup_logging


def main():
    """Run the gatherer."""
    opts = arg_parse()

    logger = setup_logging(opts, "geographic_gatherer")

    print("Setting timezone to UTC")
    os.environ["TZ"] = "UTC"
    time.tzset()

    granule_triggers = GeographicGatherer(opts)
    status = granule_triggers.run()

    logger.info("GeographicGatherer has stopped.")

    return status


if __name__ == '__main__':
    status = main()
    sys.exit(status)
