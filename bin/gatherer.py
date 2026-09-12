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
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
