"""The tests package."""

import unittest
# import doctest
from pytroll_collectors.tests import (test_helper_functions,
                                      test_scisys,
                                      test_triggers,
                                      test_segments)


def suite():
    """Test suite."""
    mysuite = unittest.TestSuite()
    # Test the documentation strings
    # mysuite.addTests(doctest.DocTestSuite(image))
    # Use the unittests also
    mysuite.addTests(test_helper_functions.suite())
    mysuite.addTests(test_scisys.suite())
    mysuite.addTests(test_triggers.suite())
    mysuite.addTests(test_segments.suite())

    return mysuite
