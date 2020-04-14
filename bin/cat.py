#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2014, 2015 Martin Raspaud
#
# Author(s):
#
#   Martin Raspaud <martin.raspaud@smhi.se>
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

"""Cat segments together."""

import argparse
import logging
import logging.config
from six.moves.configparser import RawConfigParser, NoOptionError
from subprocess import Popen, PIPE
import threading
import os
import tempfile
from bz2 import BZ2File
from datetime import datetime, timedelta

from trollsift.parser import compose
from posttroll.publisher import Publish
from posttroll.subscriber import Subscribe
from posttroll.message import Message

logger = logging.getLogger(__name__)


def arg_parse():
    """Handle input arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--log",
                        help="File to log to (defaults to stdout)",
                        default=None)
    parser.add_argument("-v", "--verbose", help="print debug messages too",
                        action="store_true")
    parser.add_argument("-c", "--config_item", dest="config_item",
                        type=str,
                        default='',
                        help="The item in the file with configuration.")
    parser.add_argument("config", help="config file to be used")

    return parser.parse_args()


def reader(stream, log_func):
    """Read the stream."""
    while True:
        s = stream.readline()
        if not s:
            break
        log_func(s.strip())
    stream.close()


class bunzipped(object):
    """Bunzipped context."""

    def __init__(self, files, **kwargs):
        """Init the context."""
        super(bunzipped, self).__init__(**kwargs)
        self._files = files
        self._to_del = []

    def __enter__(self):
        """Enter the context."""
        filenames = []
        for filename in sorted(self._files):
            if filename.endswith(".bz2"):
                logger.debug("bunzipping %s...", filename)
                tmp_fd, tmp_filename = tempfile.mkstemp()
                tmp_file = os.fdopen(tmp_fd, "wb")
                try:
                    with BZ2File(filename) as bzfile:
                        tmp_file.write(bzfile.read())
                    filenames.append(tmp_filename)
                finally:
                    self._to_del.append(tmp_filename)
                    tmp_file.close()
            else:
                filenames.append(filename)
        return filenames

    def __exit__(self, *args, **kwargs):
        """Exit the context."""
        for filename in self._to_del:
            try:
                logger.debug("deleting %s...", filename)
                os.remove(filename)
            except OSError as err:
                logger.debug("Can't remove %s: %s", filename, str(err))


def popen(cmd):
    """Run cmd in Popen."""
    p = Popen(cmd.split(), stderr=PIPE, stdout=PIPE)
    out_reader = threading.Thread(target=reader, args=(p.stdout, logger.info))
    err_reader = threading.Thread(target=reader, args=(p.stderr, logger.error))
    out_reader.start()
    err_reader.start()
    out_reader.join()
    err_reader.join()


def get_aliases(raw_config_str):
    """Get the aliases from the config."""
    items = raw_config_str.split("|")
    aliases = {}
    for item in items:
        key, vals = item.split(":")
        aliases[key] = dict([alias.split("=") for alias in vals.split(",")])
    return aliases


def process_message(msg, config):
    """Process the message."""
    pattern = config["output_file_pattern"]
    input_files = [item["uri"] for item in msg.data["collection"]]

    data = msg.data.copy()
    data["proc_time"] = datetime.utcnow()
    try:
        aliases = get_aliases(config["aliases"])
    except KeyError:
        aliases = {}
    for key in aliases:
        if key in data:
            data[key] = aliases[key].get(data[key], data[key])

    try:
        min_length = int(config.get('min_length'))
    except NoOptionError:
        min_length = 0
    if data["end_time"] - data["start_time"] < timedelta(minutes=min_length):
        logger.info('Pass too short, skipping: %s to %s',
                    str(data["start_time"]), str(data["end_time"]))
        return

    output_file = compose(pattern, data)

    with bunzipped(input_files) as files_to_read:
        keyvals = {"input_files": " ".join(files_to_read),
                   "output_file": output_file}
        cmd_pattern = config["command"]
        cmd = compose(cmd_pattern, keyvals)
        logger.info("Running %s", cmd)

        if "stdout" in config:
            stdout_file = compose(config["stdout"], keyvals)
            with open(stdout_file, "w") as output:
                p = Popen(cmd.split(), stderr=PIPE, stdout=output)
                err_reader = threading.Thread(target=reader,
                                              args=(p.stderr, logger.error))
                err_reader.start()
                err_reader.join()

        else:
            popen(cmd)

    msg.type = "file"
    new_data = msg.data.copy()
    del new_data["collection"]
    new_data["filename"] = os.path.basename(output_file)
    new_data["uri"] = output_file
    if "publish_topic" in config:
        msg.subject = config.get("publish_topic")
    msg2 = Message(msg.subject, "file", new_data)

    return msg2


def setup_logging(log_fname, verbose):
    """Setup logging."""
    global logger

    if log_fname:
        handler = logging.handlers.TimedRotatingFileHandler(log_fname,
                                                            "midnight",
                                                            backupCount=7)
    else:
        handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[%(levelname)s: %(asctime)s :"
                                           " %(name)s] %(message)s",
                                           '%Y-%m-%d %H:%M:%S'))
    if verbose:
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO
    handler.setLevel(loglevel)
    logging.getLogger('').setLevel(loglevel)
    logging.getLogger('').addHandler(handler)
    logging.getLogger("posttroll").setLevel(logging.INFO)
    logger = logging.getLogger("cat")


def main():
    """Make cat run."""
    opts = arg_parse()

    setup_logging(opts.log, opts.verbose)

    cfg = RawConfigParser()
    cfg.read(opts.config)
    config = dict(cfg.items(opts.config_item))

    services = ''
    if 'services' in config:
        services = config['services'].split(',')

    nameservers = []
    if 'nameservers' in config:
        nameservers = config['nameservers'].split(',')

    publish_port = 0
    if 'publish_port' in config:
        publish_port = int(config['publish_port'])

    sub_nameserver = 'localhost'
    if 'subscriber_nameserver' in config:
        sub_nameserver = config['subscriber_nameserver']

    sub_addresses = None
    if 'subscriber_addresses' in config:
        sub_addresses = config['subscriber_addresses'].split(',')

    try:
        with Publish("cat_" + opts.config_item, port=publish_port,
                     nameservers=nameservers) as pub:
            with Subscribe(services, topics=config["topic"],
                           addr_listener=True, addresses=sub_addresses,
                           nameserver=sub_nameserver) as sub:
                for msg in sub.recv(2):
                    if msg is None:
                        continue
                    if msg.type == "collection":
                        new_msg = str(process_message(msg, config))
                        if new_msg is None:
                            continue
                        logger.info("Sending %s", new_msg)
                        pub.send(new_msg)
    except KeyboardInterrupt:
        logging.shutdown()
    finally:
        print("Thank you for using pytroll/cat!"
              "See you soon on pytroll.org.")


if __name__ == '__main__':
    main()
