#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2017 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <adam.dybbroe@smhi.se>

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

"""Posttroll runner for the zipcollector.

Listen to messages with datasets (from gatherer) and zip files in the dataset
together into one tar file and store on disk in configurable destination directory.

"""

import logging
import os
import shutil
import sys
import tarfile
from datetime import timedelta

import yaml
from six.moves.urllib.parse import urlparse

import posttroll.subscriber
from posttroll.publisher import Publish
from trollsift.parser import compose

LOG = logging.getLogger(__name__)

#: Default time format
_DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

#: Default log format
_DEFAULT_LOG_FORMAT = '[%(levelname)s: %(asctime)s : %(name)s] %(message)s'

ENV_MODE = os.getenv("ENV_MODE")
if ENV_MODE is None:
    ENV_MODE = "offline"


PLATFORM_NAME = {'Meteosat-10': 'met10',
                 'Meteosat-11': 'met11',
                 'Meteosat-9': 'met09',
                 'Meteosat-8': 'met08'}


def get_arguments():
    """Get command line arguments.

    Return name of the service and the config filepath
    """
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument('-c', '--config_file',
                        type=str,
                        dest='config_file',
                        default='',
                        help="The file containing " +
                        "configuration parameters e.g. zipcollector.cfg")
    parser.add_argument("-s", "--service",
                        help="Name of the service (e.g. 0deg-lvl1",
                        dest="service",
                        type=str,
                        default="unknown")
    parser.add_argument("-v", "--verbose",
                        help="print debug messages too",
                        action="store_true")

    args = parser.parse_args()

    if args.config_file == '':
        print("Configuration file required! zipcollector.py <file>")
        sys.exit()
    if args.service == '':
        print("Service required! Use command-line switch -s <service>")
        sys.exit()
    else:
        service = args.service.lower()

    if 'template' in args.config_file:
        print("Template file given as master config, aborting!")
        sys.exit()

    return service, args.config_file


def get_config(configfile, service, procenv):
    """Get the configuration from file."""
    with open(configfile, 'r') as fp_:
        config = yaml.load(fp_)

    options = {}
    for item in config:
        if not isinstance(config[item], dict):
            options[item] = config[item]
        elif item in [service]:
            for key in config[service]:
                if not isinstance(config[service][key], dict):
                    options[key] = config[service][key]
                elif key in [procenv]:
                    for memb in config[service][key]:
                        options[memb] = config[service][key][memb]

    return options


def start_zipcollector(registry, message, options, **kwargs):
    """From a posttroll (gatherer) message start the pytroll zip collector."""
    del kwargs
    outdir_destination = options['destination_output_dir']
    outdir_local = options['local_output_dir']
    requested_tslots = options['requested_timeslots']

    LOG.info("")
    LOG.info("registry dict: " + str(registry))
    LOG.info("\tMessage:")
    LOG.info(message)

    if message is None:
        return registry
    elif (message.type != 'dataset'):
        LOG.warning(
            "Message type is not a collection! Type=%s", str(message.type))
        return registry

    if 'start_time' in message.data:
        start_time = message.data['start_time']
        scene_id = start_time.strftime('%Y%m%d%H%M')
    else:
        LOG.error("No start time in message!")
        start_time = None
        return registry

    if 'end_time' in message.data:
        end_time = message.data['end_time']
    else:
        LOG.warning("No end time in message!")
        end_time = start_time + timedelta(seconds=60 * 12)  # noqa

    if 'seviri' not in message.data['sensor']:
        LOG.debug("Scene is not supported")
        LOG.warning(
            "Sensor {0} is not SEVIRI! Continue".format(str(message.data['sensor'])))
        return registry
    else:
        registry[scene_id] = len(message.data['dataset'])

    # Now check that the time slot is among those requested
    LOG.debug("Wanted time slots: %s", str(requested_tslots))
    wanted_timeslot = False
    if '%.2d' % start_time.minute in requested_tslots:
        wanted_timeslot = True

    if wanted_timeslot:
        LOG.info("Time slot {0} is requested".format(start_time))

        # Example filename:
        # (service=0deg-lvl1)
        # met10___hritglob1708171100.tgz
        satid = PLATFORM_NAME.get(
            message.data['platform_name'], 'met10')
        filename = compose(options['archive_filename'],
                           {'start_time': start_time, 'satid': satid})

        local_filepath = os.path.join(outdir_local, filename)
        dest_filepath = os.path.join(
            outdir_destination, filename + '_original')

        # Create the tar archive:
        LOG.debug("Create gzipped tar archive: %s", local_filepath)
        status = True
        try:
            with tarfile.open(local_filepath, "w|gz") as archive:
                for item in message.data['dataset']:
                    filepath = urlparse(item['uri']).path
                    archive.add(filepath, arcname=item['uid'])

            copy_file_to_destination(local_filepath, dest_filepath)
            monitor_msg = "File successfully created"
        except Exception as err:
            monitor_msg = "Failed generating tar file: " + str(err)
            status = False

        if 'monitoring_hook' in options:
            options['monitoring_hook'](status, monitor_msg)
        else:
            LOG.error("Configuration lacking a monitoring_hook entry!")

    else:
        LOG.info("Time slot {0} NOT requested. Do nothing".format(start_time))

    return registry


def copy_file_to_destination(inpath, outpath):
    """Copy a file.

    Copy a file from one destination (typical local disk or a SAN type disk storage)
    to another (typically NFS based filesystem) using tempfile

    """
    import tempfile
    tmp_filepath = tempfile.mktemp(suffix='_' + os.path.basename(outpath),
                                   dir=os.path.dirname(outpath))
    shutil.copy(inpath, tmp_filepath)
    os.rename(tmp_filepath, outpath)

    return


def zipcollector_live_runner(options):
    """Listen and trigger processing."""
    LOG.info("*** Start the zipcollector runner:")
    LOG.debug("Listens for messages of type: %s", options['message_type'])
    with posttroll.subscriber.Subscribe('', [options['message_type'], ], True) as subscr:
        with Publish('zipcollector_runner', 0) as publisher:
            file_reg = {}
            for msg in subscr.recv():
                file_reg = start_zipcollector(
                    file_reg, msg, options, publisher=publisher)
                # Cleanup in file registry (keep only the last 5):
                keys = list(file_reg.keys())
                if len(keys) > 5:
                    keys.sort()
                    file_reg.pop(keys[0])


if __name__ == "__main__":

    (service_name, config_filename) = get_arguments()

    OPTIONS = get_config(config_filename, service_name, ENV_MODE)

    MAIL_HOST = 'localhost'
    SENDER = OPTIONS.get('mail_sender', 'jon.doe@fort.nox')
    MAIL_FROM = '"Pytroll-zipcollector error" <' + str(SENDER) + '>'
    try:
        RECIPIENTS = OPTIONS.get("mail_subscribers").split()
    except AttributeError:
        print("Recipients must be set! Exit...")
        sys.exit(9)

    MAIL_TO = RECIPIENTS
    MAIL_SUBJECT = 'New Critical Event From the pytroll-zipcollector'

    from logging import handlers
    handler = logging.StreamHandler(sys.stderr)

    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt=_DEFAULT_LOG_FORMAT,
                                  datefmt=_DEFAULT_TIME_FORMAT)
    handler.setFormatter(formatter)
    logging.getLogger('').addHandler(handler)
    logging.getLogger('').setLevel(logging.DEBUG)
    logging.getLogger('posttroll').setLevel(logging.INFO)

    smtp_handler = handlers.SMTPHandler(MAIL_HOST,
                                        MAIL_FROM,
                                        MAIL_TO,
                                        MAIL_SUBJECT)
    smtp_handler.setLevel(logging.CRITICAL)
    logging.getLogger('').addHandler(smtp_handler)

    LOG = logging.getLogger('zipcollector_runner')

    zipcollector_live_runner(OPTIONS)
