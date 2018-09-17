#!/usr/bin/env python
# -*- coding: utf-8 -*-

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

"""Unit testing for global mosaic
"""

import unittest
import os
import os.path
import datetime as dt
import time
from threading import Thread

import numpy as np

from pyresample.geometry import AreaDefinition
from pyresample.utils import _get_proj4_args
from posttroll import message
from posttroll.ns import NameServer

import pytroll_collectors.global_mosaic as gm

THIS_DIR = os.path.dirname(os.path.abspath(__file__))

ADEF = AreaDefinition("EPSG4326", "EPSG:4326", "EPSG:4326",
                      _get_proj4_args("init=EPSG:4326"),
                      200, 100,
                      (-180., -90., 180., 90.))


class TestWorldCompositeDaemon(unittest.TestCase):

    adef = ADEF

    tslot = dt.datetime(2016, 10, 12, 12, 0)
    # Images from individual satellites
    sat_fnames = [os.path.join(THIS_DIR, "data", fname) for fname in
                  ["20161012_1200_GOES-15_EPSG4326_wv.png",
                   "20161012_1200_GOES-13_EPSG4326_wv.png",
                   "20161012_1200_Meteosat-10_EPSG4326_wv.png",
                   "20161012_1200_Meteosat-8_EPSG4326_wv.png",
                   "20161012_1200_Himawari-8_EPSG4326_wv.png"]]

    # Image with all satellites merged without blending
    unblended = os.path.join(THIS_DIR, "data",
                             "20161012_1200_EPSG4326_wv_no_blend.png")
    # Empty image
    empty_image = os.path.join(THIS_DIR, "data", "empty.png")

    def setUp(self):
        # Start a nameserver
        self.ns_ = NameServer(max_age=dt.timedelta(seconds=3))
        self.thr = Thread(target=self.ns_.run)
        self.thr.start()

    def tearDown(self):
        # Stop nameserver
        self.ns_.stop()
        self.thr.join()

    def test_WorldCompositeDaemon(self):
        """Test WorldCompositeDaemon"""

        # Test incoming message handling and saving

        # Epoch: message sending time
        config = {"topics": ["/test"], "area_def": ADEF,
                  "timeout_epoch": "message", "timeout": 45,
                  "num_expected": 5,
                  "out_pattern": os.path.join(THIS_DIR, "data",
                                              "test_out.png")
                  }

        comp = gm.WorldCompositeDaemon(config)

        # There should be no slots
        self.assertEqual(len(comp.slots), 0)

        for i in range(len(self.sat_fnames)):
            msg = message.Message("/test", "file",
                                  {"uri": self.sat_fnames[i],
                                   "nominal_time": self.tslot,
                                   "productname": "wv"})
            epoch = msg.time
            comp._handle_message(msg)

            # Number of slots
            self.assertEqual(len(comp.slots), 1)

            # Number of composites
            self.assertEqual(len(comp.slots[self.tslot]), 1)

            # Number of files
            self.assertEqual(comp.slots[self.tslot]["wv"]["num"], i + 1)

            # Timeout
            diff = (comp.slots[self.tslot]["wv"]["timeout"] - (epoch +
                    dt.timedelta(minutes=config["timeout"])))
            self.assertAlmostEqual(diff.total_seconds(), 0.0, places=2)

            comp._check_timeouts_and_save()

            # Saving should not happen before all the images are received
            if i < 4:
                self.assertEqual(comp.slots[self.tslot]["wv"]["num"], i + 1)
            else:
                # After fifth image the composite should be saved and
                # all composites and slots removed
                self.assertEqual(len(comp.slots), 0)
                self.assertTrue(os.path.exists(config["out_pattern"]))
                # Remove the file
                os.remove(config["out_pattern"])

        comp.stop()

        # Epoch: file nominal time
        config = {"topics": ["/test"], "area_def": ADEF,
                  "timeout_epoch": "nominal_time", "timeout": 45,
                  "num_expected": 5,
                  "out_pattern": os.path.join(THIS_DIR, "data",
                                              "test_out.png")
                  }

        comp = gm.WorldCompositeDaemon(config)

        for i in range(len(self.sat_fnames)):
            msg = message.Message("/test", "file",
                                  {"uri": self.sat_fnames[i],
                                   "nominal_time": self.tslot,
                                   "productname": "wv"})
            epoch = self.tslot
            comp._handle_message(msg)

            # Number of slots
            self.assertEqual(len(comp.slots), 1)

            # Number of files should be one every time
            self.assertEqual(comp.slots[self.tslot]["wv"]["num"], 1)

            # Timeout
            self.assertEqual(comp.slots[self.tslot]["wv"]["timeout"],
                             (epoch +
                              dt.timedelta(minutes=config["timeout"])))

            # Output file should be present after the first run
            if i > 0:
                self.assertTrue(os.path.exists(config["out_pattern"]))

            comp._check_timeouts_and_save()

            # There shouldn't be any slots now
            self.assertEqual(len(comp.slots), 0)

        # Remove the file
        os.remove(config["out_pattern"])

        # Stop compositor daemon
        comp.stop()


class TestGlobalMosaic(unittest.TestCase):

    adef = ADEF

    tslot = dt.datetime(2016, 10, 12, 12, 0)
    # Images from individual satellites
    sat_fnames = [os.path.join(THIS_DIR, "data", fname) for fname in
                  ["20161012_1200_GOES-15_EPSG4326_wv.png",
                   "20161012_1200_GOES-13_EPSG4326_wv.png",
                   "20161012_1200_Meteosat-10_EPSG4326_wv.png",
                   "20161012_1200_Meteosat-8_EPSG4326_wv.png",
                   "20161012_1200_Himawari-8_EPSG4326_wv.png"]]

    # Image with all satellites merged without blending
    unblended = os.path.join(THIS_DIR, "data",
                             "20161012_1200_EPSG4326_wv_no_blend.png")
    # Empty image
    empty_image = os.path.join(THIS_DIR, "data", "empty.png")

    def test_calc_pixel_mask_limits(self):
        """Test calculation of pixel mask limits"""
        # Mask data from edges
        lon_limits = [-25., 25.]
        result = gm.calc_pixel_mask_limits(self.adef, lon_limits)
        self.assertTrue(np.all(np.array(result) ==
                               np.array([[0, 86], [113, 200]])))

        # Data wraps around 180 lon, mask from the middle of area
        lon_limits = [170., -170.]
        result = gm.calc_pixel_mask_limits(self.adef, lon_limits)
        self.assertTrue(np.all(np.array(result) == np.array([[5, 194]])))

    def test_read_image(self):
        """Test reading and masking images"""
        # Non-existent image
        result = gm.read_image("asdasd.png", self.adef, lon_limits=None)
        self.assertIsNone(result)

        # Read empty image, check that all channel data and mask values are 0
        result = gm.read_image(self.empty_image, self.adef, lon_limits=None)
        self.assertTrue(np.all(np.isnan(np.array(result))))

    def test_create_world_composite(self):
        """Test world composite creation"""

        def _compare_images(img1, img2):
            """Compare data and masks for each channel"""
            # Smallest step for 8-bit input data
            min_step = 1. / 255.
            diff = np.array(np.abs(img1 - img2))
            self.assertTrue(np.nanmax(diff) <= min_step)

        # All satellites with built-in longitude limits
        result = gm.create_world_composite(self.sat_fnames,
                                           self.adef, gm.LON_LIMITS,
                                           img=None)
        correct = gm.read_image(self.unblended, self.adef, lon_limits=None)

        # Check that attributes are set correctly
        self.assertEqual(result.area, self.adef)
        self.assertEqual(result.shape, correct.shape)

        _compare_images(result, correct)

        # All satellites with no longitude limits
        result = gm.create_world_composite(self.sat_fnames,
                                           self.adef, gm.LON_LIMITS,
                                           img=None)
        correct = gm.read_image(self.unblended, self.adef, lon_limits=None)
        self.assertEqual(result.shape, correct.shape)
        _compare_images(result, correct)


def suite():
    """The suite for test_global_mosaic
    """
    loader = unittest.TestLoader()
    mysuite = unittest.TestSuite()
    mysuite.addTest(loader.loadTestsFromTestCase(TestWorldCompositeDaemon))
    mysuite.addTest(loader.loadTestsFromTestCase(TestGlobalMosaic))

    return mysuite

if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=2).run(suite())
