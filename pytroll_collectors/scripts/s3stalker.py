#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2020, 2023 Martin Raspaud

# Author(s):

#   Martin Raspaud <martin.raspaud@smhi.se>

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
"""S3 stalker.

This script fetches filenames newer than a given timedelta and publishes the url
of the corresponding file. It exits after that.
So for now, this script is meant to be run at regular intervals, for example
with a cronjob.
"""

import logging.config

from pytroll_collectors.s3stalker import publish_new_files, get_configs_from_command_line


def main():
    """Stalk an s3 bucket."""
    bucket, config, log_config = get_configs_from_command_line()

    if log_config:
        logging.config.dictConfig(log_config)

    try:
        publish_new_files(bucket, config)
    except KeyboardInterrupt:
        print("terminating publisher...")


if __name__ == '__main__':
    main()
