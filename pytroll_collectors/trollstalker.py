"""Trollstalker module."""
import argparse
import datetime as dt
import logging
import os
import sys
import time
from collections import OrderedDict, deque
from configparser import RawConfigParser
import warnings

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from posttroll.message import Message
from posttroll.publisher import create_publisher_from_dict_config
from pytroll_collectors import helper_functions
from trollsift import Parser, compose

logger = logging.getLogger(__name__)


RUNNING = True


def stop():
    """Stop trollstalker."""
    global RUNNING
    RUNNING = False


def main(command_args=None):
    """Run Trollstalker.

    Commandline parsing and stalker startup.
    """
    observer = start_observer(command_args)

    try:
        while RUNNING:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping TrollStalker")
    finally:
        stop_observer(observer)


def start_observer(command_args):
    """Start observing files and process them."""
    os.environ["TZ"] = "UTC"
    time.tzset()

    monitored_dirs, settings = get_settings(command_args)

    event_processor = EventProcessor(**settings)

    event_handler = WatchdogHandler(event_processor)
    observer = Observer(generate_full_events=True)

    for monitored_dir in monitored_dirs:
        os.makedirs(monitored_dir, exist_ok=True)
        observer.schedule(event_handler, monitored_dir, recursive=True)
    observer.start()
    return observer


def stop_observer(observer):
    """Stop the observer."""
    observer.stop()
    observer.join()


def get_settings(command_args):
    """Get the trollstalker settings."""
    args = parse_args(command_args)

    # Parse commandline arguments.  If args are given, they override
    # the configuration file.

    # Check first commandline arguments
    monitored_dirs = args.monitored_dirs
    if monitored_dirs == '':
        monitored_dirs = None

    posttroll_port = args.posttroll_port
    topic = args.topic
    event_names = args.event_names
    instrument = args.instrument
    nameservers = args.nameservers
    config_item = args.config_item

    filepattern = args.filepattern
    if args.filepattern == '':
        filepattern = None

    if args.configuration_file is not None:
        config_fname = args.configuration_file

        if "template" in config_fname:
            print("Template file given as trollstalker logging config,"
                  " aborting!")
            sys.exit()

        config = RawConfigParser()
        config.read(config_fname)
        config = OrderedDict(config.items(args.config_item))
        config['name'] = args.configuration_file

        topic = topic or config['topic']
        monitored_dirs = monitored_dirs or config['directory'].split(",")
        filepattern = filepattern or config['filepattern']
        try:
            posttroll_port = posttroll_port or int(config['posttroll_port'])
        except (KeyError, ValueError):
            if posttroll_port is None:
                posttroll_port = 0
        try:
            filepattern = filepattern or config['filepattern']
        except KeyError:
            pass
        try:
            event_names = event_names or config['event_names']
        except KeyError:
            pass
        try:
            instrument = instrument or config['instruments']
        except KeyError:
            pass
        try:
            history = int(config['history'])
        except KeyError:
            history = 0

        try:
            nameservers = nameservers or config['nameservers']
        except KeyError:
            nameservers = []

        aliases = helper_functions.parse_aliases(config)
        tbus_orbit = bool(config.get("tbus_orbit", False))

        granule_length = float(config.get("granule", 0))

        custom_vars = parse_vars(config)

        try:
            log_config = config["stalker_log_config"]
        except KeyError:
            try:
                loglevel = getattr(logging, config["loglevel"])
                if loglevel == "":
                    raise AttributeError
            except AttributeError:
                loglevel = logging.DEBUG
            logger.setLevel(loglevel)

            strhndl = logging.StreamHandler()
            strhndl.setLevel(loglevel)
            log_format = "[%(asctime)s %(levelname)-8s %(name)s] %(message)s"
            formatter = logging.Formatter(log_format)

            strhndl.setFormatter(formatter)
            logger.addHandler(strhndl)
        else:
            logging.config.fileConfig(log_config)

    if event_names:
        warnings.warn("Event names is deprecated and is now ignored. Files are detected on write close and moving in.",
                      DeprecationWarning, stacklevel=2)

    logger.debug("Logger started")

    if not isinstance(monitored_dirs, list):
        monitored_dirs = [monitored_dirs]

    if nameservers:
        if nameservers.lower() == "false":
            nameservers = False
        else:
            nameservers = nameservers.split(',')
    else:
        nameservers = []

    settings = dict()
    settings["topic"] = topic
    settings["instrument"] = instrument
    settings["config_item"] = config_item
    settings["posttroll_port"] = posttroll_port
    settings["filepattern"] = filepattern
    settings["aliases"] = aliases
    settings["tbus_orbit"] = tbus_orbit
    settings["history_length"] = history
    settings["granule_length"] = granule_length
    settings["custom_vars"] = custom_vars
    settings["nameservers"] = nameservers
    return monitored_dirs, settings


def parse_args(command_args):
    """Parse the command line arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument("-d", "--monitored_dirs", dest="monitored_dirs",
                        nargs='+',
                        type=str,
                        default=[],
                        help="Names of the monitored directories "
                        "separated by space")
    parser.add_argument("-p", "--posttroll_port", dest="posttroll_port",
                        default=0, type=int,
                        help="Local port where messages are published")
    parser.add_argument("-t", "--topic", dest="topic",
                        type=str,
                        default=None,
                        help="Topic of the sent messages")
    parser.add_argument("-c", "--configuration_file",
                        type=str,
                        help="Name of the config.ini configuration file")
    parser.add_argument("-C", "--config_item",
                        type=str,
                        help="Name of the configuration item to use")
    parser.add_argument("-e", "--event_names",
                        type=str, default=None,
                        help="Name of the pyinotify events to monitor")
    parser.add_argument("-f", "--filepattern",
                        type=str,
                        help="Filepattern used to parse "
                        "satellite/orbit/date/etc information")
    parser.add_argument("-i", "--instrument",
                        type=str, default=None,
                        help="Instrument name in the satellite")
    parser.add_argument("-n", "--nameservers",
                        type=str, default=None,
                        help="Posttroll nameservers to register own address,"
                        " otherwise multicasting is used")

    args = parser.parse_args(command_args)
    return args


class WatchdogHandler(FileSystemEventHandler):
    """A watchdog handler do detect incomming files."""

    def __init__(self, processor):
        """Set up the handler."""
        self.processor = processor

    def on_closed(self, event):
        """Trigger processing on closed write."""
        self.processor.process(event)
        print("yep, processed")

    def on_moved(self, event):
        """Trigger processing on move."""
        self.processor.process(event)


class EventProcessor:
    """A processor for events."""

    def __init__(self, topic, instrument, config_item, posttroll_port=0, filepattern=None,
                 aliases=None, tbus_orbit=False, history_length=0, granule_length=0,
                 custom_vars=None, nameservers=[]):  # noqa
        """Set up the event processor."""
        pub_settings = dict(name="trollstalker_" + config_item,
                            port=posttroll_port,
                            nameservers=nameservers)
        self.pub = create_publisher_from_dict_config(pub_settings)

        self.pub.start()
        self.topic = topic
        if filepattern is None:
            filepattern = '{filename}'
        self.file_parser = Parser(filepattern)
        self.instrument = instrument
        self.aliases = aliases
        self.custom_vars = custom_vars
        self.tbus_orbit = tbus_orbit
        self.granule_length = granule_length
        self._history = deque([], history_length)

    def process(self, event):
        """Process the event."""
        try:
            pathname = event.dest_path or event.src_path
        except AttributeError:
            pathname = event.src_path
        logger.debug("processing %s", pathname)
        info = self.parse_file_info(pathname)
        if len(info) > 0:
            # Check if this file has been recently dealt with
            if pathname not in self._history:
                self._history.append(pathname)
                message = self.create_message(info)
                logger.info("Publishing message %s", str(message))
                self.pub.send(str(message))
            else:
                logger.debug("Data has been published recently, skipping.")

    def create_message(self, info):
        """Create broadcasted message."""
        return Message(self.topic, 'file', dict(info))

    def parse_file_info(self, pathname):
        """Parse satellite and orbit information from the filename.

        Message is sent, if a matching filepattern is found.
        """
        logger.debug("filter: %s\t event: %s", self.file_parser.fmt, pathname)
        pathname_join = os.path.basename(pathname)
        if 'origin_inotify_base_dir_skip_levels' in self.custom_vars:
            # TODO wtf
            pathname_list = pathname.split('/')
            pathname_join = "/".join(pathname_list[int(self.custom_vars['origin_inotify_base_dir_skip_levels']):])
        else:
            logger.debug("No origin_inotify_base_dir_skip_levels in self.custom_vars")

        info = OrderedDict()

        try:
            info.update(self.file_parser.parse(pathname_join))
            logger.debug("Extracted info from filename: %s", str(info))
        except ValueError:
            # Filename didn't match pattern, so empty the info dict
            logger.debug("Couldn't extract any useful information from filename")
        else:
            info['uri'] = pathname
            info['uid'] = os.path.basename(pathname)
            info['sensor'] = self.instrument.split(',')
            logger.debug("info['sensor']: " + str(info['sensor']))

            if self.tbus_orbit and "orbit_number" in info:
                logger.debug("Changing orbit number by -1!")
                info["orbit_number"] -= 1

            # replace values with corresponding aliases, if any are given
            if self.aliases:
                keys = info.copy().keys()
                for key in keys:
                    if key in self.aliases:
                        info['orig_' + key] = info[key]
                        info[key] = self.aliases[key][str(info[key])]

            # add start_time and end_time if not present
            try:
                base_time = info["time"]
            except KeyError:
                try:
                    base_time = info["nominal_time"]
                except KeyError:
                    base_time = info["start_time"]
            if "start_time" not in info:
                info["start_time"] = base_time
            if "start_date" in info:
                info["start_time"] = \
                    dt.datetime.combine(info["start_date"].date(),
                                        info["start_time"].time())
                if "end_date" not in info:
                    info["end_date"] = info["start_date"]
                del info["start_date"]
            if "end_date" in info:
                info["end_time"] = \
                    dt.datetime.combine(info["end_date"].date(),
                                        info["end_time"].time())
                del info["end_date"]
            if "end_time" not in info and self.granule_length > 0:
                info["end_time"] = base_time + \
                    dt.timedelta(seconds=self.granule_length)

            if "end_time" in info:
                while info["start_time"] > info["end_time"]:
                    info["end_time"] += dt.timedelta(days=1)

            if self.custom_vars is not None:
                for var_name in self.custom_vars:
                    var_pattern = self.custom_vars[var_name]
                    var_val = None
                    if '%' in var_pattern:
                        var_val = helper_functions.create_aligned_datetime_var(
                            var_pattern, info)
                    if var_val is None:
                        var_val = compose(var_pattern, info)
                    info[var_name] = var_val
        return info

    def stop(self):
        """Stop the publisher."""
        self.pub.stop()


def parse_vars(config):
    """Parse custom variables from the config.

    Aliases are given in the config as:

    {'var_<name>': 'value'}

    where <name> is the name of the key which value will be
    added to metadata. <value> is a trollsift pattern.

    """
    variables = OrderedDict()

    for key in config:
        if 'var_' in key:
            new_key = key.replace('var_', '')
            var = config[key]
            variables[new_key] = var
    return variables
