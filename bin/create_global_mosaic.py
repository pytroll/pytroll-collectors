#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import time
import yaml
import logging
import logging.config
import logging.handlers

from pytroll_collectors.global_mosaic import WorldCompositeDaemon


def main():
    """main()"""

    with open(sys.argv[1], "r") as fid:
        config = yaml.load(fid)

    try:
        if config["use_utc"]:
            os.environ["TZ"] = "UTC"
            time.tzset()
    except KeyError:
        pass

    logging.config.dictConfig(config["log_config"])
    logger = logging.getLogger("WorldComposite")

    logger.debug("Logger started")

    # Create and start compositor
    compositor = WorldCompositeDaemon(config)
    compositor.set_logger(logger)
    try:
        compositor.run()
    finally:
        if compositor._listener is not None:
            compositor._listener.stop()
        if compositor._publisher is not None:
            compositor._publisher.stop()


if __name__ == "__main__":
    main()
