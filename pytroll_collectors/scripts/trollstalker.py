"""Trollstalker script.

./trollstalker.py -c /path/to/trollstalker_config.ini -C noaa_hrpt
"""

import logging.config

from pytroll_collectors.trollstalker import main

if __name__ == "__main__":
    logger = logging.getLogger("trollstalker")
    main()
