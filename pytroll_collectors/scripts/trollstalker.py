"""Trollstalker script.

./trollstalker.py -c /path/to/trollstalker_config.ini -C noaa_hrpt
"""

from pytroll_collectors.logging import setup_logging
from pytroll_collectors.trollstalker import parse_args
from pytroll_collectors.trollstalker import main


if __name__ == "__main__":
    opts = parse_args(args)
    logger = setup_logging(opts, "trollstalker")
    main()
