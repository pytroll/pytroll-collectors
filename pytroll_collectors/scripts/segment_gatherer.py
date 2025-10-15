"""Segment gatherer."""

import argparse
import os
import time

from pytroll_collectors.segments import SegmentGatherer
from pytroll_collectors.segments import ini_to_dict
from pytroll_collectors.helper_functions import read_yaml
from pytroll_collectors.logging import setup_logging


def arg_parse(args=None):
    """Handle input arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--log",
                        help="File to log to (defaults to stdout)",
                        default=None)
    parser.add_argument("-v", "--verbose", help="print debug messages too",
                        action="store_true")
    parser.add_argument("-c", "--config", help="config file to be used")
    parser.add_argument("-C", "--config_item",
                        help="config item to use with .ini files")

    return parser.parse_args(args)


def main():
    """Parse cmdline, read config etc."""
    args = arg_parse()

    if args.config_item:
        config = ini_to_dict(args.config, args.config_item)
    else:
        config = read_yaml(args.config)

    print("Setting timezone to UTC")
    os.environ["TZ"] = "UTC"
    time.tzset()

    setup_logging(args, "segment_gatherer")

    gatherer = SegmentGatherer(config)

    try:
        gatherer.run()
    finally:
        gatherer.stop()


if __name__ == "__main__":
    main()
