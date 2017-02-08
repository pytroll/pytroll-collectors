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
import datetime as dt

import numpy as np
from PIL import Image

from pytroll_collectors import image_scaler as sca
from mpop.imageo.geo_image import GeoImage


class TestImageScaler(unittest.TestCase):

    data = np.random.randint(0, 255, (100, 100), dtype=np.uint8)
    img_l = Image.fromarray(data, mode='L')
    img_la = Image.fromarray(np.dstack((data, data)), mode='LA')
    img_rgb = Image.fromarray(np.dstack((data, data, data)), mode='RGB')
    img_rgba = Image.fromarray(np.dstack((data, data, data, data)),
                               mode='RGBA')

    def test_get_crange(self):
        def test_vals(res):
            for val in res:
                self.assertEqual(val[0], 0)
                self.assertEqual(val[1], 1)

        res = sca._get_crange(1)
        test_vals(res)
        self.assertTrue(len(res) == 1)

        res = sca._get_crange(2)
        test_vals(res)

        res = sca._get_crange(3)
        self.assertTrue(len(res) == 3)
        test_vals(res)

    def test_pil_to_geoimage(self):
        res = sca._pil_to_geoimage(self.img_l, None, None, fill_value=None)
        self.assertEqual(res.mode, 'L')
        self.assertTrue(res.fill_value is None)
        self.assertTrue(res.time_slot is None)
        self.assertTrue(res.area is None)
        tslot = dt.datetime(2017, 2, 7, 12, 0)
        res = sca._pil_to_geoimage(self.img_la, None, tslot, fill_value=0)
        self.assertEqual(res.mode, 'LA')
        self.assertTrue(res.fill_value[0] is 0)
        self.assertEqual(res.time_slot, tslot)
        res = sca._pil_to_geoimage(self.img_rgb, None, None, fill_value=None)
        self.assertEqual(res.mode, 'RGB')
        res = sca._pil_to_geoimage(self.img_rgba, None, None, fill_value=None)
        self.assertEqual(res.mode, 'RGBA')


def suite():
    """The suite for test_global_mosaic
    """
    loader = unittest.TestLoader()
    mysuite = unittest.TestSuite()
    mysuite.addTest(loader.loadTestsFromTestCase(TestImageScaler))

    return mysuite

if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=2).run(suite())
