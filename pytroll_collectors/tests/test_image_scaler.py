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
import tempfile
import os
import os.path
from ConfigParser import ConfigParser

import numpy as np
from PIL import Image, ImageFont

from pytroll_collectors import image_scaler as sca
from mpop.imageo.geo_image import GeoImage


class TestImageScaler(unittest.TestCase):

    data = np.random.randint(0, 255, (100, 100), dtype=np.uint8)
    img_l = Image.fromarray(data, mode='L')
    img_la = Image.fromarray(np.dstack((data, data)), mode='LA')
    img_rgb = Image.fromarray(np.dstack((data, data, data)), mode='RGB')
    img_rgba = Image.fromarray(np.dstack((data, data, data, data)),
                               mode='RGBA')

    config = ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__),
                             'data', 'scale_images.ini'))

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

    def test_save_image(self):
        out_dir = tempfile.gettempdir()
        fname = os.path.join(out_dir, 'img.png')
        sca.save_image(self.img_rgb, fname)
        fname = os.path.join(out_dir, 'img.tif')
        sca.save_image(self.img_rgb, fname)

    def test_crop_image(self):
        res = sca.crop_image(self.img_rgb, (3, 3, 7, 7))
        self.assertEqual(res.size[0], 4)
        self.assertEqual(res.size[1], 4)
        res = sca.crop_image(self.img_rgb, (-3, -3, 700, 700))
        self.assertEqual(res.size[0], 100)
        self.assertEqual(res.size[1], 100)

    def test_resize_image(self):
        res = sca.resize_image(self.img_rgb, (30, 30))
        self.assertEqual(res.size[0], 30)
        self.assertEqual(res.size[1], 30)
        res = sca.resize_image(self.img_rgb, (300, 300))
        self.assertEqual(res.size[0], 300)
        self.assertEqual(res.size[1], 300)

    def test_get_text_settings(self):
        # No text settings in config, should give default values
        res = sca._get_text_settings(self.config, '/empty/text/settings')
        self.assertTrue(res['loc'] == 'SW')
        self.assertTrue(res['font_fname'] is None)
        self.assertEqual(res['font_size'], 12)
        for i in range(3):
            self.assertEqual(res['text_color'][i], 0)
            self.assertEqual(res['bg_color'][i], 255)
        res = sca._get_text_settings(self.config, '/text/settings')
        self.assertEqual(res['x_marginal'], 10)
        self.assertEqual(res['y_marginal'], 3)
        self.assertEqual(res['bg_extra_width'], 5)

    def test_get_font(self):
        res = sca._get_font('non_existent', 12)
        self.assertTrue(isinstance(res, ImageFont.ImageFont))
        res = sca._get_font(os.path.join(os.path.dirname(__file__),
                                         'data', 'DejaVuSerif.ttf'), 12)
        self.assertTrue(isinstance(res, ImageFont.FreeTypeFont))

    def test_add_text(self):
        text_settings = sca._get_text_settings(self.config, '/text/settings')
        # Replace placeholder font path with one that certainly exists
        text_settings['font_fname'] = os.path.join(os.path.dirname(__file__),
                                                   'data', 'DejaVuSerif.ttf')
        # Default text settings (black on white)
        res = sca.add_text(self.img_l, 'PL', text_settings)
        self.assertTrue(res.mode == 'L')
        res = sca.add_text(self.img_la, 'PL', text_settings)
        self.assertTrue(res.mode == 'LA')
        res = sca.add_text(self.img_rgb, 'PL', text_settings)
        self.assertTrue(res.mode == 'RGB')
        res = sca.add_text(self.img_rgba, 'PL', text_settings)
        self.assertTrue(res.mode == 'RGBA')

        # Black on blue
        text_settings['bg_color'] = (200, 200, 255)
        res = sca.add_text(self.img_l, 'PL', text_settings)
        self.assertTrue(res.mode == 'RGB')
        res = sca.add_text(self.img_la, 'PL', text_settings)
        self.assertTrue(res.mode == 'RGBA')
        res = sca.add_text(self.img_rgb, 'PL', text_settings)
        self.assertTrue(res.mode == 'RGB')
        res = sca.add_text(self.img_rgba, 'PL', text_settings)
        self.assertTrue(res.mode == 'RGBA')

    def test_is_rgb_color(self):
        res = sca._is_rgb_color(((0, 0, 0), ))
        self.assertFalse(res)
        res = sca._is_rgb_color(((1, 0, 0), ))
        self.assertTrue(res)
        res = sca._is_rgb_color(((0, 0, 0), (1, 0, 0), ))
        self.assertTrue(res)

    def test_get_text_and_box_locations(self):
        shape = self.img_rgb.size
        textsize = (18, 11)
        marginals = (10, 3)
        bg_extra_width = 4

        text_loc, box_loc = \
            sca._get_text_and_box_locations(shape, 'SW',
                                            textsize, marginals,
                                            bg_extra_width)

        # Test only relevant things: x and y corners and that box is
        # wider than text
        self.assertEqual(text_loc[0], 10)
        self.assertEqual(text_loc[1],
                         shape[1] - textsize[1] - 2 * marginals[1])
        self.assertLessEqual(box_loc[0], text_loc[0])
        self.assertEqual(box_loc[1], text_loc[1])
        self.assertGreaterEqual(box_loc[2], text_loc[0] + textsize[0])
        self.assertEqual(box_loc[3], shape[1])

        text_loc, box_loc = \
            sca._get_text_and_box_locations(shape, 'NE',
                                            textsize, marginals,
                                            bg_extra_width)

        # Test only relevant things: x and y corners and that box is
        # wider than text
        self.assertEqual(text_loc[0],
                         shape[0] - textsize[0] - marginals[0])
        self.assertEqual(text_loc[1], 0)
        self.assertLessEqual(box_loc[0], text_loc[0])
        self.assertEqual(box_loc[1], text_loc[1])
        self.assertGreaterEqual(box_loc[2], text_loc[0] + textsize[0])
        self.assertGreaterEqual(box_loc[3], textsize[1] - 1)

        text_loc, box_loc = \
            sca._get_text_and_box_locations(shape, 'SC',
                                            textsize, marginals,
                                            bg_extra_width)

        # Test only centering
        self.assertEqual(text_loc[0], (shape[0] - textsize[0]) / 2)
        self.assertLessEqual(box_loc[0], text_loc[0])
        self.assertGreaterEqual(box_loc[2], text_loc[0] + textsize[0])

    def test_adjust_img_mode_for_text(self):
        res = sca._adjust_img_mode_for_text(self.img_l, ((0, 0, 0), ))
        self.assertTrue(res.mode == 'L')
        res = sca._adjust_img_mode_for_text(self.img_l, ((1, 0, 0), ))
        self.assertTrue(res.mode == 'RGB')
        res = sca._adjust_img_mode_for_text(self.img_la, ((1, 0, 0), ))
        self.assertTrue(res.mode == 'RGBA')
        res = sca._adjust_img_mode_for_text(self.img_rgb, ((1, 0, 0), ))
        self.assertTrue(res.mode == 'RGB')
        res = sca._adjust_img_mode_for_text(self.img_rgba, ((1, 0, 0), ))
        self.assertTrue(res.mode == 'RGBA')

    def test_read_image(self):
        pass

    def test_read_image(self):
        pass

    def test_add_overlays(self):
        pass


def suite():
    """The suite for test_global_mosaic
    """
    loader = unittest.TestLoader()
    mysuite = unittest.TestSuite()
    mysuite.addTest(loader.loadTestsFromTestCase(TestImageScaler))

    return mysuite

if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=2).run(suite())
