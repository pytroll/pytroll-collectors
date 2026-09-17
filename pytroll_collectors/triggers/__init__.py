#!/usr/bin/env python

"""Triggers for region_collectors."""

import logging

from ._posttroll import PostTrollTrigger  # noqa: F401

logger = logging.getLogger(__name__)

try:
    from ._watchdog import WatchDogTrigger
except ImportError:
    logger.exception("Watchdog import failed!")
    WatchDogTrigger = None
