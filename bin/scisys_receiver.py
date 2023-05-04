#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012 - 2023 Pytroll developers
#
# Author(s):
#
#   Martin Raspaud <martin.raspaud@smhi.se>
#   Janne Kotro <janne.kotro@fmi.fi>
#   Panu Lahtinen <panu.lahtinen@fmi.fi>
#   Adam Dybbroe <Firstname.Lastname at smhi.se>
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

"""Receiver for 2met messages through zeromq.

Outputs messages with the following metadata:

- satellite
- format
- start_time
- end_time
- filename
- uri
- type
- orbit_number
- [instrument, number]

"""
import logging
import logging.handlers
import argparse
from pytroll_collectors.scisys import receive_from_zmq
from pytroll_collectors.helper_functions import get_local_ips

logger = logging.getLogger(__name__)


def parse_args():
    """Parse commandline arguments."""
    local_ips = get_local_ips()
    local_ips.remove('127.0.0.1')

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config",
                        help="YAML config file to use.")
    parser.add_argument("-P", "--publish-port", type=int, default=0,
                        dest="publish_port", help="Publish port")
    parser.add_argument("-n", "--nameserver", nargs='+', default=[],
                        dest="nameservers",
                        help="Nameserver(s) to connect to")
    parser.add_argument("-l", "--log", help="File to log to",
                        dest="log", default=None)
    parser.add_argument("-f", "--ftp_prefix", dest="ftp_prefix",
                        type=str,
                        help="FTP path prefix for message uri")
    parser.add_argument("-t", "--target_server", dest="target_server",
                        type=str,
                        nargs='*',
                        default=local_ips,
                        help="IP of the target server."
                        "In case of multiple dispatches in GMC."
                        "Defaults to the local host.")

    return parser.parse_args()


def setup_logging(log_file=None):
    """Set up logging."""
    global logger
    if log_file:
        handler = logging.handlers.TimedRotatingFileHandler(log_file,
                                                            "midnight",
                                                            backupCount=7)
    else:
        handler = logging.StreamHandler()

    handler.setFormatter(logging.Formatter("[%(levelname)s: %(asctime)s :"
                                           " %(name)s] %(message)s",
                                           '%Y-%m-%d %H:%M:%S'))
    handler.setLevel(logging.DEBUG)
    logging.getLogger('').setLevel(logging.DEBUG)
    logging.getLogger('').addHandler(handler)
    logging.getLogger("posttroll").setLevel(logging.INFO)
    logger = logging.getLogger("receiver")


def main():
    """Run scisys receiver."""
    opts = parse_args()

    configfile = opts.config

    # no_sats = opts.excluded_satellites

    setup_logging(log_file=opts.log)

    try:
        receive_from_zmq(configfile,
                         opts.target_server, opts.ftp_prefix,
                         publish_port=opts.publish_port,
                         nameservers=opts.nameservers, days=1)
    except KeyboardInterrupt:
        pass
    except Exception:
        logger.exception("Something wrong happened...")
    finally:
        print("Thank you for using pytroll/receiver."
              " See you soon on pytroll.org!")


if __name__ == '__main__':
    main()
