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

"""Obsolete gatherer script."""

import time
from importlib.util import spec_from_file_location, module_from_spec
import os.path

spec = spec_from_file_location(
    "geographic_gatherer",
    os.path.join(os.path.dirname(__file__), "geographic_gatherer.py"))
geographic_gatherer = module_from_spec(spec)
spec.loader.exec_module(geographic_gatherer)


if __name__ == '__main__':
    print("\nThe 'gatherer.py' script is deprecated.\n\n"
          "Please use 'geographic_gatherer.py' instead\n")
    time.sleep(10)
    geographic_gatherer.main()
