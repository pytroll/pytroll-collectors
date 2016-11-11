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
import numpy as np
from threading import Thread
import time

from pyresample.geometry import AreaDefinition
from pyresample.utils import _get_proj4_args
from mpop.imageo.geo_image import GeoImage
from posttroll import message
from posttroll.ns import NameServer

import pytroll_collectors.global_mosaic as gm

THIS_DIR = os.path.dirname(os.path.abspath(__file__))

ADEF = AreaDefinition("EPSG4326", "EPSG:4326", "EPSG:4326",
                      _get_proj4_args("init=EPSG:4326"),
                      200, 100,
                      (-180., -90., 180., 90.))


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
    # Image with two satellites merged with blending, no scaling
    blended_not_scaled = \
        os.path.join(THIS_DIR, "data",
                     "20161012_1200_EPSG4326_wv_blend_no_scale.png")
    # Image with two satellites merged with blending and scaling
    blended_scaled = \
        os.path.join(THIS_DIR, "data",
                     "20161012_1200_EPSG4326_wv_blend_and_scale.png")
    # Empty image
    empty_image = os.path.join(THIS_DIR, "data", "empty.png")

    def test_calc_pixel_mask_limits(self):
        """Test calculation of pixel mask limits"""
        # Mask data from edges
        lon_limits = [-25., 25.]
        result = gm.calc_pixel_mask_limits(self.adef, lon_limits)
        self.assertItemsEqual(result, [[0, 86], [113, 200]])

        # Data wraps around 180 lon, mask from the middle of area
        lon_limits = [170., -170.]
        result = gm.calc_pixel_mask_limits(self.adef, lon_limits)
        self.assertItemsEqual(result, [[5, 194]])

    def test_read_image(self):
        """Test reading and masking images"""
        # Non-existent image
        result = gm.read_image("asdasd.png", self.tslot, self.adef,
                               lon_limits=None)
        self.assertIsNone(result)

        # Read empty image, check that all channel data and mask values are 0
        result = gm.read_image(self.empty_image, self.tslot, self.adef,
                               lon_limits=None)
        correct = np.zeros((self.adef.y_size, self.adef.x_size),
                           dtype=np.float32)
        for chan in result.channels:
            self.assertTrue(np.all(chan.data == correct))
            self.assertFalse(np.any(chan.mask == correct.astype(np.bool)))

    def test_create_world_composite(self):
        """Test world composite creation"""

        # Start a nameserver
        ns_ = NameServer(max_age=dt.timedelta(seconds=3))
        thr = Thread(target=ns_.run)
        thr.start()

        def _compare_images(img1, img2):
            """Compare data and masks for each channel"""
            # Smallest step for 8-bit input data
            min_step = 1. / 255.
            for i in range(4):
                diff = np.abs(img1.channels[i] - img2.channels[i])
                self.assertTrue(np.all(diff <= min_step))

        # All satellites with built-in longitude limits
        result = gm.create_world_composite(self.sat_fnames, self.tslot,
                                           self.adef, gm.LON_LIMITS,
                                           blend=None, img=None)
        correct = gm.read_image(self.unblended,
                                self.tslot, self.adef, lon_limits=None)

        # Check that attributes are set correctly
        self.assertEqual(result.area, correct.area)
        self.assertEqual(result.time_slot, correct.time_slot)

        _compare_images(result, correct)

        # All satellites with no longitude limits
        result = gm.create_world_composite(self.sat_fnames, self.tslot,
                                           self.adef, gm.LON_LIMITS,
                                           blend=None, img=None)
        correct = gm.read_image(self.unblended,
                                self.tslot, self.adef, lon_limits=None)
        _compare_images(result, correct)

        # Two satellites, erosion and smoothing, no scaling
        blend = {"erosion_width": 40, "smooth_width": 40, "scale": False}
        result = gm.create_world_composite(self.sat_fnames[1:3], self.tslot,
                                           self.adef, None,
                                           blend=blend, img=None)
        correct = gm.read_image(self.blended_not_scaled,
                                self.tslot, self.adef, lon_limits=None)
        _compare_images(result, correct)

        # Two satellites, erosion, smoothing and scaling
        blend = {"erosion_width": 40, "smooth_width": 40, "scale": True}
        result = gm.create_world_composite(self.sat_fnames[1:3], self.tslot,
                                           self.adef, None,
                                           blend=blend, img=None)
        correct = gm.read_image(self.blended_scaled,
                                self.tslot, self.adef, lon_limits=None)
        _compare_images(result, correct)

        # Stop nameserver
        ns_.stop()
        thr.join()
        time.sleep(2)

    def test_WorldCompositeDaemon(self):
        """Test WorldCompositeDaemon"""

        # Start a nameserver
        ns_ = NameServer(max_age=dt.timedelta(seconds=3))
        thr = Thread(target=ns_.run)
        thr.start()

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

        # Stop nameserver
        ns_.stop()
        thr.join()
        time.sleep(2)


def suite():
    """The suite for test_global_mosaic
    """
    loader = unittest.TestLoader()
    mysuite = unittest.TestSuite()
    mysuite.addTest(loader.loadTestsFromTestCase(TestGlobalMosaic))

    return mysuite

if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=2).run(suite())
