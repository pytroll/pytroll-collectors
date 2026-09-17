#!/usr/bin/env python

"""Obsolete gatherer script."""

import time
from importlib.util import spec_from_file_location, module_from_spec
import os.path

spec = spec_from_file_location(
    "geographic_gatherer",
    os.path.join(os.path.dirname(__file__), "geographic_gatherer.py"))
geographic_gatherer = module_from_spec(spec)
spec.loader.exec_module(geographic_gatherer)


if __name__ == '__main__':
    print("\nThe 'gatherer.py' script is deprecated.\n\n"
          "Please use 'geographic_gatherer.py' instead\n")
    time.sleep(10)
    geographic_gatherer.main()
