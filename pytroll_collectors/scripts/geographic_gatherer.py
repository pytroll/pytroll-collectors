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
