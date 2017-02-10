# -*- coding: utf-8 -*-

# Copyright (c) 2017

# Author(s):

#   Panu Lahtinen <panu.lahtinen@fmi.fi>

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

import Queue
import os
import os.path
from ConfigParser import NoOptionError, NoSectionError
import logging
import logging.config
import datetime as dt
import glob
from urlparse import urlparse

import numpy as np
from PIL import Image, ImageDraw, ImageFont

from posttroll.listener import ListenerContainer
from pycoast import ContourWriter
from trollsift import parse, compose
from mpop.projector import get_area_def

try:
    from mpop.imageo.geo_image import GeoImage
except ImportError:
    GeoImage = None

GSHHS_DATA_ROOT = os.environ['GSHHS_DATA_ROOT']

# TODO: fix config parsing

DEFAULT_SECTION_VALUES = {'update_existing': False,
                          'is_backup': False,
                          'crops': [],
                          'sizes': [],
                          'tags': [],
                          'timeliness': 10,
                          'static_image_fname': None,
                          'use_platform_name_hack': False,
                          'text_pattern': None,
                          'area_def': None,
                          'overlay_config_fname': None
                          }

DEFAULT_TEXT_SETTINGS = {'text_location': 'SW',
                         'font_fname': '',
                         'font_size': '12',
                         'text_color': '0,0,0',
                         'text_bg_color': '255,255,255',
                         'x_marginal': '10',
                         'y_marginal': '3',
                         'bg_extra_width': '0',
                         }


class ImageScaler(object):

    '''Class for scaling images to defined sizes.'''

    # Config options for the current received message
    out_dir = ''
    update_existing = False
    is_backup = False
    subject = None
    crops = []
    sizes = []
    tags = []
    timeliness = 10
    static_image_fname = None
    areaname = None
    in_pattern = None
    fileparts = {}
    out_pattern = None
    text_pattern = None
    text_settings = None
    area_def = None
    overlay_config = None
    filepath = None
    existing_fname_parts = None

    def __init__(self, config):
        self.config = config
        topics = config.sections()
        self.listener = ListenerContainer(topics=topics)
        self._loop = True
        self._overlays = {}
        self._cw = ContourWriter(GSHHS_DATA_ROOT)

    def stop(self):
        '''Stop scaler before shutting down.'''
        if self._loop:
            self._loop = False
            if self.listener is not None:
                self.listener.stop()

    def _update_current_config(self):
        """Update the current config to class attributes."""

        # These are mandatory config items, so handle them first
        self._get_mandatory_config_items()

        self._parse_crops()
        self._parse_sizes()
        self._parse_tags()
        self._tidy_platform_name()
        self._get_text_settings()

        self.out_dir = self.config.get(self.subject, 'out_dir', 0,
                                       DEFAULT_SECTION_VALUES)

        self.update_existing = self._get_bool('update_existing')

        self.is_backup = self._get_bool('only_backup')

        self.timeliness = int(self.config.get(self.subject, 'timeliness', 0,
                                              DEFAULT_SECTION_VALUES))

        self.static_image_fname = \
            self.config.get(self.subject, "static_image_fname", 0,
                            DEFAULT_SECTION_VALUES)

        self.overlay_config = self.config.get(self.subject,
                                              'overlay_config', 0,
                                              DEFAULT_SECTION_VALUES)

    def _get_bool(self, key):
        """Get *key* from config and interpret it as boolean"""
        val = self.config.get(self.subject, key, 0, DEFAULT_SECTION_VALUES)
        return val.lower() in ['yes', '1', 'true']

    def _get_text_settings(self):
        """Parse text overlay pattern and text settings"""
        self.text_pattern = self.config.get(self.subject, 'text', 0,
                                            DEFAULT_SECTION_VALUES)
        self.text_settings = _get_text_settings(self.config, self.subject)

    def _get_mandatory_config_items(self):
        """Get mandatory config items and log possible errors"""
        try:
            self.areaname = self.config.get(self.subject, 'areaname')
            in_pattern = self.config.get(self.subject, 'in_pattern')
            self.in_pattern = in_pattern.replace('{areaname}', self.areaname)
            out_pattern = self.config.get(self.subject, 'out_pattern')
            self.out_pattern = os.path.join(self.out_dir, out_pattern)
        except NoOptionError:
            logging.error("Required option missing!")
            logging.error("Check that 'areaname', 'in_pattern' and " +
                          "'out_pattern' are all defined under section " +
                          self.subject)
            raise NoOptionError
        except NoSectionError:
            logging.error("No config section for message subject " +
                          self.subject)
            raise NoSectionError

    def _tidy_platform_name(self):
        """Remove "-" from platform names"""
        tidy = self._get_bool('tidy_platform_name')
        if tidy:
            self.fileparts['platform_name'] = \
                self.fileparts['platform_name'].replace('-', '')

    def _parse_crops(self):
        """Parse crop settings from the raw crop config"""
        crop_conf = self.config.get(self.subject, 'crops', 0,
                                    DEFAULT_SECTION_VALUES)
        if isinstance(crop_conf, list):
            self.crops = crop_conf
            return

        self.crops = []
        for crop in crop_conf.split(','):
            if 'x' in crop and '+' in crop:
                # Crop strings are formated like this:
                # <x_size>x<y_size>+<x_start>+<y_start>
                # eg. 1000x300+103+200
                # Origin (0, 0) is at top-left
                parts = crop.split('+')
                crop = tuple(map(int, parts[1:]) +
                             map(int, parts[0].split('x')))

                self.crops.append(crop)
            else:
                self.crops.append(None)

    def _parse_sizes(self):
        """Parse crop settings from crop config"""
        size_conf = self.config.get(self.subject, 'sizes', 0,
                                    DEFAULT_SECTION_VALUES)
        if isinstance(size_conf, list):
            self.sizes = size_conf
            return

        self.sizes = []
        for size in size_conf.split(','):
            self.sizes.append(map(int, size.split('x')))

    def _parse_tags(self):
        """Parse tags from tag config"""
        tag_conf = self.config.get(self.subject, 'tags', 0,
                                   DEFAULT_SECTION_VALUES)

        if isinstance(tag_conf, list):
            self.tags = tag_conf
            return
        self.tags = [tag for tag in tag_conf.split(',')]

    def add_overlays(self, img):
        """Add overlays to image.  Add to cache, if not already there."""
        if self.overlay_config is None:
            return img

        if self.subject not in self._overlays:
            logging.debug("Adding overlay to cache")
            area_def = get_area_def(self.areaname)
            self._overlays[self.subject] = \
                self._cw.add_overlay_from_config(self.overlay_config,
                                                 area_def)
        else:
            logging.debug("Using overlay from cache")

        try:
            return add_image_as_overlay(img, self._overlays[self.subject])
        except ValueError:
            return img

    def _check_existing(self, start_time):
        """Check if there's an existing image that should be updated"""

        # check if something silmiar has already been made:
        # checks for: platform_name, areaname and
        # start_time +- timeliness minutes
        check_start_time = start_time - \
            dt.timedelta(minutes=self.timeliness)
        check_dict = self.fileparts.copy()
        check_dict["tag"] = self.tags[0]
        if self.is_backup:
            check_dict["platform_name"] = '*'
            check_dict["sat_loc"] = '*'
        check_dict["composite"] = '*'

        first_overpass = True
        update_fname_parts = None
        for i in range(2 * self.timeliness + 1):
            check_dict['time'] = check_start_time + dt.timedelta(minutes=i)
            glob_pattern = compose(os.path.join(self.out_dir,
                                                self.out_pattern),
                                   check_dict)
            glob_fnames = glob.glob(glob_pattern)
            if len(glob_fnames) > 0:
                first_overpass = False
                logging.debug("Found files: %s", str(glob_fnames))
                try:
                    update_fname_parts = parse(self.out_pattern,
                                               glob_fnames[0])
                    update_fname_parts["composite"] = \
                        self.fileparts["composite"]
                    if not self.is_backup:
                        try:
                            update_fname_parts["platform_name"] = \
                                self.fileparts["platform_name"]
                        except KeyError:
                            pass
                    break
                except ValueError:
                    logging.debug("Parsing failed for update_fname_parts.")
                    logging.debug("out_pattern: %s, basename: %s",
                                  self.out_pattern, glob_fnames[0])
                    update_fname_parts = {}

        if self.is_backup and not first_overpass:
            logging.info("File already exists, no backuping needed.")
            return None

    def run(self):
        '''Start waiting for messages.

        On message arrival, read the image, scale down to the defined
        sizes and add coastlines.
        '''

        while self._loop:
            # Wait for new messages
            try:
                msg = self.listener.queue.get(True, 5)
            except KeyboardInterrupt:
                self.stop()
                raise
            except Queue.Empty:
                continue

            logging.info("New message with topic %s", msg.subject)

            self.subject = msg.subject
            self.filepath = urlparse(msg.data["uri"]).path

            try:
                self._update_current_config()
            except (NoOptionError, NoSectionError):
                logging.warning("Skip processing for this message.")
                continue

            # parse filename parts from the incoming file
            try:
                self.fileparts = parse(self.in_pattern,
                                       os.path.basename(self.filepath))
            except ValueError:
                logging.info("Filepattern doesn't match, skipping.")
                continue
            self.fileparts['areaname'] = self.areaname

            self.existing_fname_parts = \
                self._check_existing(msg.data["start_time"])

            # There is already a matching image which isn't going to
            # be updated
            if self.existing_fname_parts is None:
                continue

            # Read the image
            img = read_image(self.filepath)

            # Add overlays, if any
            img = self.add_overlays(img)

            # Save image(s)
            self.save_images(img)

    def save_images(self, img):
        """Save image(s)"""
        # Loop through different image sizes
        for i in range(len(self.sizes)):

            # Crop the image
            img = crop_image(img, self.crops[i])

            # Resize the image
            img = resize_image(img, self.sizes[i])

            # Update existing image if configured to do so
            if self.update_existing:
                img, fname = self._update_existing_img(img, self.tags[i])
                # Add text
                img_out = self._add_text(img, update_img=True)
            # In other case, save as a new image
            else:
                # Add text
                img_out = self._add_text(img, update_img=False)
                # Compose filename
                self.fileparts['tag'] = self.tags[i]
                fname = compose(self.out_pattern, self.fileparts)

            # Save image
            img_out.save(fname)

            # Update static image, if given in config
            self._update_static_img(img, tags[i])

    def _update_static_img(self, img, tag):
        """Update image with static filename"""
        if self.static_image_fname is None:
            return

        fname = compose(os.path.join(self.out_dir,
                                     self.static_image_fname),
                        self.fileparts)
        img = self._update_existing_img(img, tag, fname=fname)
        img = self._add_text(img, update_img=False)

        img.save(fname)
        logging.info("Updated image with static filename: %s", fname)

    def _add_text(self, img, update_img=False):
        """Add text to the given image"""
        if self.text_pattern is None:
            return img

        if update_img:
            text = compose(self.text_pattern, self.existing_fname_parts)
        else:
            text = compose(self.text_pattern, self.fileparts)

        return add_text(img, text, self.text_settings)

    def _update_existing_img(self, img, tag, fname=None):
        """Update existing image"""
        if fname is None:
            self.existing_fname_parts['tag'] = tag
            fname = compose(os.path.join(self.out_dir, self.out_pattern),
                            self.existing_fname_parts)
        logging.info("Updating image %s with image %s",
                     fname, self.filepath)
        img_out = update_existing_image(fname, img)

        return img_out


def resize_image(img, size):
    """Resize given image to size (x_size, y_size)"""
    x_res, y_res = size

    if img.size[0] == x_res and img.size[1] == y_res:
        img_out = img
    else:
        img_out = img.resize((x_res, y_res))

    return img_out


def crop_image(img, crop):
    """Crop the given image"""
    # Adjust limits so that they don't exceed image dimensions
    crop = list(crop)
    if crop[0] < 0:
        crop[0] = 0
    if crop[1] < 0:
        crop[1] = 0
    if crop[2] > img.size[0]:
        crop[2] = img.size[0]
    if crop[3] > img.size[1]:
        crop[3] = img.size[1]

    try:
        if crop is not None:
            img_wrk = img.crop(crop)
        else:
            img_wrk = img
    except IndexError:
        img_wrk = img

    return img_wrk


def save_image(img, fname, adef=None, time_slot=None, fill_value=None):
    """Save image.  In case of area definition and start time are given,
    and the image type is tif, convert first to Geoimage to save geotiff
    """
    if (adef is not None and time_slot is not None and
            fname.lower().endswith(('.tif', '.tiff'))):
        img = _pil_to_geoimage(img, adef=adef, time_slot=time_slot,
                               fill_value=fill_value)
    img.save(fname)


def _pil_to_geoimage(img, adef, time_slot, fill_value=None):
    """Convert PIL image to GeoImage"""
    # Get image mode, widht and height
    mode = img.mode
    width = img.width
    height = img.height

    # TODO: handle other than 8-bit images
    max_val = 255.
    # Convert to Numpy array
    img = np.array(img.getdata()).astype(np.float32)
    img = img.reshape((height, width, len(mode)))

    chans = []
    # TODO: handle P image mode
    if mode == 'L':
        chans.append(np.squeeze(img) / max_val)
    else:
        if mode.endswith('A'):
            mask = img[:, :, -1]
        else:
            mask = False
        for i in range(len(mode)):
            chans.append(np.ma.masked_where(mask, img[:, :, i] / max_val))

    return GeoImage(chans, adef, time_slot, fill_value=fill_value,
                    mode=mode, crange=_get_crange(len(mode)))


def _get_crange(num):
    """Get crange for interval (0, 1) for *num* image channels."""
    tupl = (0., 1.)
    return num * (tupl, )


def _get_text_settings(config, subject):
    """Parse text settings from the config."""
    settings = {}
    settings['loc'] = config.get(subject, 'text_location', 0,
                                 DEFAULT_TEXT_SETTINGS)
    font_fname = config.get(subject, 'font_fname', 0,
                            DEFAULT_TEXT_SETTINGS)
    if font_fname == '':
        settings['font_fname'] = None
    else:
        settings['font_fname'] = font_fname
    settings['font_size'] = int(config.get(subject, 'font_size', 0,
                                           DEFAULT_TEXT_SETTINGS))
    text_color = config.get(subject, 'text_color', 0,
                            DEFAULT_TEXT_SETTINGS)
    settings['text_color'] = \
        tuple([int(x) for x in text_color.split(',')])
    bg_color = config.get(subject, 'text_bg_color', 0, DEFAULT_TEXT_SETTINGS)
    settings['bg_color'] = tuple([int(x) for x in bg_color.split(',')])
    settings['x_marginal'] = int(config.get(subject, 'x_marginal', 0,
                                            DEFAULT_TEXT_SETTINGS))
    settings['y_marginal'] = int(config.get(subject, 'y_marginal', 0,
                                            DEFAULT_TEXT_SETTINGS))
    settings['bg_extra_width'] = int(config.get(subject, 'bg_extra_width', 0,
                                                DEFAULT_TEXT_SETTINGS))

    return settings


def add_text(img, text, settings):
    """Add text to the image"""
    # TODO: replace with pydecorate?
    img = _adjust_img_mode_for_text(img, (settings['text_color'],
                                          settings['bg_color']))
    # Use 3-tuples only for RGB and RGBA, for others use integers
    if len(img.mode) < 3:
        text_color = settings['text_color'][0]
        bg_color = settings['bg_color'][0]
    else:
        text_color = settings['text_color']
        bg_color = settings['bg_color']

    draw = ImageDraw.Draw(img)
    font = _get_font(settings['font_fname'], settings['font_size'])
    textsize = draw.textsize(text, font)

    marginals = (settings['x_marginal'], settings['y_marginal'])
    loc = settings['loc']
    bg_extra_width = settings['bg_extra_width']
    text_loc, box_loc = _get_text_and_box_locations(img.size, loc,
                                                    textsize, marginals,
                                                    bg_extra_width)

    draw.rectangle(box_loc, fill=bg_color)
    draw.text(text_loc, text, fill=text_color, font=font)

    return img


def _get_text_and_box_locations(img_shape, loc, textsize, marginals,
                                bg_extra_width):
    """Get text and text box locations in the image based on the config"""

    if 'S' in loc:
        text_loc, box_loc = _text_in_south(img_shape, loc,
                                           textsize, marginals,
                                           bg_extra_width)
    else:
        text_loc, box_loc = _text_in_north(img_shape, loc, textsize,
                                           marginals, bg_extra_width)

    return text_loc, box_loc


def _text_in_north(img_shape, loc, textsize, marginals,
                   bg_extra_width):
    width, height = img_shape
    x_marginal, y_marginal = marginals

    if 'W' in loc:
        text_loc = (x_marginal, y_marginal)
        box_loc = [text_loc[0] - bg_extra_width,
                   0,
                   text_loc[0] + textsize[0] + bg_extra_width,
                   textsize[1] + 2 * y_marginal]
    elif 'E' in loc:
        text_loc = (width - textsize[0] - x_marginal, 0)  # y_marginal)
        box_loc = [text_loc[0] - bg_extra_width,
                   0,
                   text_loc[0] + textsize[0] + bg_extra_width,
                   textsize[1] + 2 * y_marginal]
    # Center
    else:
        text_loc = ((width - textsize[0]) / 2, 0)  # y_marginal)
        box_loc = [text_loc[0] - bg_extra_width,
                   0,
                   text_loc[0] + textsize[0] + bg_extra_width,
                   textsize[1] + 2 * y_marginal]

    return text_loc, box_loc


def _text_in_south(img_shape, loc, textsize, marginals,
                   bg_extra_width):
    width, height = img_shape
    x_marginal, y_marginal = marginals
    if 'W' in loc:
        text_loc = (x_marginal, height - textsize[1] - 2 * y_marginal)
        box_loc = [text_loc[0] - bg_extra_width,
                   height - textsize[1] - 2 * y_marginal,
                   text_loc[0] + textsize[0] + bg_extra_width,
                   height]
    elif 'E' in loc:
        text_loc = (width - textsize[0] - x_marginal,
                    height - textsize[1] - 2 * y_marginal)
        box_loc = [text_loc[0] - bg_extra_width,
                   height - textsize[1] - 2 * y_marginal,
                   text_loc[0] + textsize[0] + bg_extra_width,
                   height]
    # Center
    else:
        text_loc = ((width - textsize[0]) / 2,
                    height - textsize[1] - 2 * y_marginal)
        box_loc = [text_loc[0] - bg_extra_width,
                   height - textsize[1] - 2 * y_marginal,
                   text_loc[0] + textsize[0] + bg_extra_width,
                   height]

    return text_loc, box_loc


def _adjust_img_mode_for_text(img, colors):
    """Adjust image mode to mach the text settings"""
    if 'L' in img.mode:
        if _is_rgb_color(colors):
            mode = 'RGB'
            if 'A' in img.mode:
                mode += 'A'
            logging.info("Converting to %s", mode)
            img = img.convert(mode)
    return img


def _is_rgb_color(colors):
    """Return True if RGB colors are needed to represent *colors*, or
    False if grayscale is enough."""
    for col in colors:
        if np.unique(col).size > 1:
            return True
    return False


def _get_font(font_fname, font_size):
    """Load a font from the given file, or if that fails, load the default
    font from PIL"""
    try:
        font = ImageFont.truetype(font_fname, font_size)
        logging.debug('Font read from %s', font_fname)
    except (IOError, TypeError):
        try:
            font = ImageFont.load(font_fname)
            logging.debug('Font read from %s', font_fname)
        except (IOError, TypeError):
            logging.warning('Falling back to default font')
            font = ImageFont.load_default()

    return font


def update_existing_image(fname, new_img):
    '''Read image from fname, if present, and update valid data (= not
    black) from img_in.  Return updated image as PIL image.
    '''

    new_img_mode = new_img.mode
    try:
        old_img = Image.open(fname)
    except IOError:
        return new_img

    if new_img_mode == 'LA':
        old_img = np.array(old_img.convert('RGBA'))
        old_img = np.dstack((old_img[:, :, 0], old_img[:, :, -1]))
        new_img = np.array(new_img.convert('RGBA'))
        new_img = np.dstack((new_img[:, :, 0], new_img[:, :, -1]))
    else:
        old_img = np.array(old_img.convert(new_img_mode))
        new_img = np.array(new_img)

    ndims = old_img.shape
    logging.debug("Image dimensions: old_img: %s, new_img: %s", str(ndims),
                  str(new_img.shape))
    if len(ndims) > 1:
        mask = np.max(new_img, -1) > 0
        for i in range(ndims[-1]):
            old_img[mask, i] = new_img[mask, i]
    else:
        mask = new_img > 0
        old_img[mask] = new_img[mask]

    return Image.fromarray(old_img, mode=new_img_mode)


def read_image(filepath):
    """Read the image from *filepath* and return it as PIL image."""
    return Image.open(filepath)


def add_image_as_overlay(img, overlay):
    """Add PIL image as overlay to another image"""
    logging.info("Adding overlays")
    if len(img.mode) > len(overlay.mode) or 'A' not in overlay.mode:
        logging.info("Overlay needs to have same channels as the "
                     "image, AND an alpha channel")
        raise ValueError
    img.paste(overlay, mask=overlay)

    return img
