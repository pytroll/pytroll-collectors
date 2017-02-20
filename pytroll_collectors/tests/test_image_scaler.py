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
from mock import patch

import numpy as np
from PIL import Image, ImageFont

from trollsift import parse
from pytroll_collectors import image_scaler as sca


class TestImageScaler(unittest.TestCase):

    # Create fake images with different modes
    data = np.random.randint(0, 255, (100, 100), dtype=np.uint8)
    img_l = Image.fromarray(data, mode='L')
    img_la = Image.fromarray(np.dstack((data, data)), mode='LA')
    img_rgb = Image.fromarray(np.dstack((data, data, data)), mode='RGB')
    img_rgba = Image.fromarray(np.dstack((data, data, data, data)),
                               mode='RGBA')

    # Read config
    config = ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__),
                             'data', 'scale_images.ini'))

    def test_get_crange(self):
        def test_vals(res):
            for val in res:
                self.assertEqual(val[0], 0)
                self.assertEqual(val[1], 1)

        # Test that color ranges have correct
        res = sca._get_crange(1)
        test_vals(res)
        self.assertTrue(len(res) == 1)

        res = sca._get_crange(2)
        test_vals(res)

        res = sca._get_crange(3)
        self.assertTrue(len(res) == 3)
        test_vals(res)

    def test_pil_to_geoimage(self):
        res = sca._pil_to_geoimage(self.img_l.copy(), None, None)
        self.assertEqual(res.mode, 'L')
        self.assertIsNone(res.fill_value)
        self.assertIsNone(res.time_slot)
        self.assertIsNone(res.area)
        tslot = dt.datetime(2017, 2, 7, 12, 0)
        res = sca._pil_to_geoimage(self.img_la.copy(), None, tslot,
                                   fill_value=(42,))
        self.assertEqual(res.mode, 'LA')
        self.assertIsNone(res.fill_value)
        self.assertEqual(res.time_slot, tslot)
        res = sca._pil_to_geoimage(self.img_rgb.copy(), None, None,
                                   fill_value=(42, 42, 42))
        for i in range(3):
            self.assertTrue(res.fill_value[i], 42)
        self.assertEqual(res.mode, 'RGB')
        res = sca._pil_to_geoimage(self.img_rgba.copy(), None, None,
                                   fill_value=(42, 42, 42))
        self.assertEqual(res.mode, 'RGBA')
        self.assertIsNone(res.fill_value)

    def test_save_image(self):
        out_dir = tempfile.gettempdir()
        fname = os.path.join(out_dir, 'img.png')
        sca.save_image(self.img_rgba.copy(), fname)
        fname = os.path.join(out_dir, 'img.tif')
        sca.save_image(self.img_rgba.copy(), fname)

    def test_crop_image(self):
        res = sca.crop_image(self.img_rgb.copy(), (3, 3, 7, 7))
        self.assertEqual(res.size[0], 4)
        self.assertEqual(res.size[1], 4)
        res = sca.crop_image(self.img_rgb.copy(), (-3, -3, 700, 700))
        self.assertEqual(res.size[0], 100)
        self.assertEqual(res.size[1], 100)

    def test_resize_image(self):
        res = sca.resize_image(self.img_rgb.copy(), (30, 30))
        self.assertEqual(res.size[0], 30)
        self.assertEqual(res.size[1], 30)
        res = sca.resize_image(self.img_rgb.copy(), (300, 300))
        self.assertEqual(res.size[0], 300)
        self.assertEqual(res.size[1], 300)

    def test_get_text_settings(self):
        # No text settings in config, should give default values
        res = sca._get_text_settings(self.config, '/empty/text/settings')
        self.assertTrue(res['loc'] ==
                        sca.DEFAULT_TEXT_SETTINGS['text_location'])
        self.assertTrue(res['font_fname'] is None)
        self.assertEqual(res['font_size'],
                         int(sca.DEFAULT_TEXT_SETTINGS['font_size']))
        text_color = map(int,
                         sca.DEFAULT_TEXT_SETTINGS['text_color'].split(','))
        text_bg_color = \
            map(int,
                sca.DEFAULT_TEXT_SETTINGS['text_bg_color'].split(','))
        for i in range(3):
            self.assertEqual(res['text_color'][i], text_color[i])
            self.assertEqual(res['bg_color'][i], text_bg_color[i])

        # Settings are given
        res = sca._get_text_settings(self.config, '/text/settings')
        self.assertEqual(res['x_marginal'], 20)
        self.assertEqual(res['y_marginal'], 5)
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
        res = sca.add_text(self.img_l.copy(), 'PL', text_settings)
        self.assertTrue(res.mode == 'L')
        res = sca.add_text(self.img_la.copy(), 'PL', text_settings)
        self.assertTrue(res.mode == 'LA')
        res = sca.add_text(self.img_rgb.copy(), 'PL', text_settings)
        self.assertTrue(res.mode == 'RGB')
        res = sca.add_text(self.img_rgba.copy(), 'PL', text_settings)
        self.assertTrue(res.mode == 'RGBA')

        # Black on blue
        text_settings['bg_color'] = (200, 200, 255)
        res = sca.add_text(self.img_l.copy(), 'PL', text_settings)
        self.assertTrue(res.mode == 'RGB')
        res = sca.add_text(self.img_la.copy(), 'PL', text_settings)
        self.assertTrue(res.mode == 'RGBA')
        res = sca.add_text(self.img_rgb.copy(), 'PL', text_settings)
        self.assertTrue(res.mode == 'RGB')
        res = sca.add_text(self.img_rgba.copy(), 'PL', text_settings)
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
        out_dir = tempfile.gettempdir()
        fname = os.path.join(out_dir, 'img.png')
        sca.save_image(self.img_rgba.copy(), fname)
        res = sca.read_image(fname)
        res = np.array(res.getdata(), dtype=np.float32)
        src = np.array(self.img_rgba.getdata(), dtype=np.float32)
        self.assertEqual(np.max(res - src), 0)

    def test_update_existing_image(self):
        out_dir = tempfile.gettempdir()
        fname = os.path.join(out_dir, 'img.png')
        sca.save_image(self.img_rgba.copy(), fname)
        data = 255 * np.ones(self.data.shape, dtype=np.uint8)
        # Replace part of the alpha channel with zeros, so that no all of the
        # image is updated
        data[0, :] *= 0
        data_stack = np.dstack((data, data, data, data))
        new_img = Image.fromarray(data_stack, mode='RGBA')
        res = sca.update_existing_image(fname, new_img)
        res = np.array(res)
        self.assertTrue(np.all(res[1:, :, :] == 255))
        self.assertTrue(np.all(res[0, :, :-1] ==
                               np.array(self.img_rgba)[0, :, :-1]))

        # Update L with L
        sca.save_image(self.img_l.copy(), fname)
        res = sca.update_existing_image(fname, self.img_l.copy())
        self.assertTrue(res.mode == 'L')
        # Update L with LA
        res = sca.update_existing_image(fname, self.img_la.copy())
        self.assertTrue(res.mode == 'LA')
        # Update L with RGB
        res = sca.update_existing_image(fname, self.img_rgb.copy())
        self.assertTrue(res.mode == 'RGB')
        # Update L with RGBA
        res = sca.update_existing_image(fname, self.img_rgba.copy())
        self.assertTrue(res.mode == 'RGBA')

        # Update LA with L
        sca.save_image(self.img_la.copy(), fname)
        res = sca.update_existing_image(fname, self.img_l.copy())
        self.assertTrue(res.mode == 'L')
        # Update LA with LA
        res = sca.update_existing_image(fname, self.img_la.copy())
        self.assertTrue(res.mode == 'LA')
        # Update LA with RGB
        res = sca.update_existing_image(fname, self.img_rgb.copy())
        self.assertTrue(res.mode == 'RGB')
        # Update LA with RGBA
        res = sca.update_existing_image(fname, self.img_rgba.copy())
        self.assertTrue(res.mode == 'RGBA')

        # Update RGB with L
        sca.save_image(self.img_rgb.copy(), fname)
        res = sca.update_existing_image(fname, self.img_l.copy())
        self.assertTrue(res.mode == 'L')
        # Update RGB with LA
        res = sca.update_existing_image(fname, self.img_la.copy())
        self.assertTrue(res.mode == 'LA')
        # Update RGB with RGB
        res = sca.update_existing_image(fname, self.img_rgb.copy())
        self.assertTrue(res.mode == 'RGB')
        # Update RGB with RGBA
        res = sca.update_existing_image(fname, self.img_rgba.copy())
        self.assertTrue(res.mode == 'RGBA')

        # Update RGBA with L
        sca.save_image(self.img_rgba.copy(), fname)
        res = sca.update_existing_image(fname, self.img_l.copy())
        self.assertTrue(res.mode == 'L')
        # Update RGBA with LA
        res = sca.update_existing_image(fname, self.img_la.copy())
        self.assertTrue(res.mode == 'LA')
        # Update RGBA with RGB
        res = sca.update_existing_image(fname, self.img_rgb.copy())
        self.assertTrue(res.mode == 'RGB')
        # Update RGBA with RGBA
        res = sca.update_existing_image(fname, self.img_rgba.copy())
        self.assertTrue(res.mode == 'RGBA')

    def test_add_image_as_overlay(self):
        res = sca.add_image_as_overlay(self.img_l.copy(), self.img_rgba)
        res = sca.add_image_as_overlay(self.img_la.copy(), self.img_rgba)
        res = sca.add_image_as_overlay(self.img_rgb.copy(), self.img_rgba)
        res = sca.add_image_as_overlay(self.img_rgba.copy(), self.img_rgba)
        data = self.data.copy()
        data[:, 10:20] = 255
        overlay = Image.fromarray(np.dstack((data, data, data, data)),
                                  mode='RGBA')
        res = sca.add_image_as_overlay(self.img_rgb.copy(), overlay)
        self.assertEqual(res.getdata(0)[10], 255)

    @patch("pytroll_collectors.image_scaler.ListenerContainer")
    @patch("pytroll_collectors.image_scaler.ContourWriter")
    def test_ImageScaler(self, cwriter, listener):
        scaler = sca.ImageScaler(self.config)
        scaler.subject = '/scaler'
        filename = '201702071200_Meteosat-10_EPSG4326_spam.png'
        filename = os.path.join(os.path.dirname(__file__),
                                'data', filename)

        res = scaler._get_conf_with_default('areaname')
        self.assertTrue(res == self.config.get('/scaler',
                                               'areaname'))

        res = scaler._get_bool('only_backup')
        self.assertTrue(res == sca.DEFAULT_CONFIG_VALUES['only_backup'])
        res = scaler._get_bool('out_dir')
        self.assertFalse(res)

        scaler._get_text_settings()
        self.assertTrue(
            scaler.text_pattern == sca.DEFAULT_CONFIG_VALUES['text_pattern'])
        self.assertTrue(isinstance(scaler.text_settings, dict))

        scaler.subject = '/empty/text/settings'
        with self.assertRaises(KeyError):
            scaler._get_mandatory_config_items()
        scaler.subject = '/not/existing'
        with self.assertRaises(KeyError):
            scaler._get_mandatory_config_items()
        scaler.subject = '/scaler'
        scaler._get_mandatory_config_items()
        self.assertTrue(scaler.areaname == self.config.get('/scaler',
                                                           'areaname'))
        self.assertTrue(scaler.in_pattern == self.config.get('/scaler',
                                                             'in_pattern'))
        self.assertTrue(scaler.out_pattern == self.config.get('/scaler',
                                                              'out_pattern'))

        scaler.fileparts.update(parse(scaler.out_pattern,
                                      os.path.basename(filename)))
        scaler._tidy_platform_name()
        self.assertTrue(scaler.fileparts['platform_name'] == "Meteosat10")

        scaler._update_current_config()
        # Test few config items that the have the default values
        self.assertEqual(scaler.timeliness,
                         sca.DEFAULT_CONFIG_VALUES['timeliness'])
        self.assertEqual(len(scaler.tags),
                         len(sca.DEFAULT_CONFIG_VALUES['tags']))
        # And the config values
        self.assertTrue(scaler.areaname == self.config.get('/scaler',
                                                           'areaname'))
        self.assertTrue(scaler.in_pattern == self.config.get('/scaler',
                                                             'in_pattern'))
        self.assertTrue(scaler.out_pattern == self.config.get('/scaler',
                                                              'out_pattern'))

        scaler._parse_crops()
        self.assertEqual(len(scaler.crops), 0)
        scaler._parse_sizes()
        self.assertEqual(len(scaler.sizes), 0)
        scaler._parse_tags()
        self.assertEqual(len(scaler.tags), 0)

        scaler.subject = '/crops/sizes/tags'
        scaler._update_current_config()
        scaler._parse_crops()
        self.assertEqual(len(scaler.crops), 2)
        self.assertEqual(len(scaler.crops[0]), 4)
        self.assertTrue(scaler.crops[1] is None)

        scaler._parse_sizes()
        self.assertEqual(len(scaler.sizes), 3)
        self.assertEqual(len(scaler.sizes[0]), 2)

        scaler._parse_tags()
        self.assertEqual(len(scaler.tags), 3)

        # Default text settings (black on white)
        res = scaler._add_text(self.img_l.copy(), 'PL')
        self.assertTrue(res.mode == 'L')
        res = scaler._add_text(self.img_la.copy(), 'PL')
        self.assertTrue(res.mode == 'LA')
        res = scaler._add_text(self.img_rgb.copy(), 'PL')
        self.assertTrue(res.mode == 'RGB')
        res = scaler._add_text(self.img_rgba.copy(), 'PL')
        self.assertTrue(res.mode == 'RGBA')

        scaler.fileparts.update(parse(scaler.out_pattern,
                                      os.path.basename(filename)))
        tslot = dt.datetime.utcnow()
        # File that doesn't exist
        res = scaler._check_existing(tslot)
        self.assertEqual(len(res), 0)
        # Existing file with "is_backup" set to False so we should get a full
        # set of metadata
        scaler.out_dir = os.path.join(os.path.dirname(__file__),
                                      'data')
        tslot = scaler.fileparts['time']
        res = scaler._check_existing(tslot)
        self.assertEqual(res['time'], tslot)
        self.assertEqual(res['areaname'], scaler.areaname)
        self.assertEqual(res['platform_name'],
                         scaler.fileparts['platform_name'])
        self.assertEqual(res['composite'], 'spam')
        # Existing file with "is_backup" set to True
        scaler.is_backup = True
        res = scaler._check_existing(tslot)
        self.assertIsNone(res)


def suite():
    """The suite for test_global_mosaic
    """
    loader = unittest.TestLoader()
    mysuite = unittest.TestSuite()
    mysuite.addTest(loader.loadTestsFromTestCase(TestImageScaler))

    return mysuite

if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=2).run(suite())
